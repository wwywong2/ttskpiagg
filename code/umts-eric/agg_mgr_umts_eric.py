#!/usr/bin/python
import os
import sys
import time
import uuid

import requests
import util


# argv[1] - starting file num
# argv[2] - ending file num
# argv[3] - output dir
# argv[4] - (optional) "cluster" or "client" mode


## Constants
max_filenum = 49 # stop process after filenum hit this
retry_sec = 3 # 120 # sec to retry when all cores are busy/used
core_per_job = 2 # core per job - parsing 1 seq file
check_interval_sec = 3 # 12 # sec to check inbetween submit of new job
max_check_ctr = 1 # max num of recheck when there is just 1 job slot left
max_num_job = 6 # max num of job allow concurrently
max_num_job_hardlimit = 20 # max num of job (hard limit)


# set some key var
curr_py_path = os.path.realpath(__file__) # current running file - abs path
curr_py_dir, curr_py_filename = os.path.split(curr_py_path)  # current file and folder - abs path
#curr_py_dir = os.path.dirname(curr_py_path)


## globals
prev_jobname = ""

# argv[1] - starting file
filenum = 0
# update starting file
if len(sys.argv) > 1:
   filenum = int(sys.argv[1])

# argv[2] - ending file
# update starting file
if len(sys.argv) > 2:
   max_filenum = int(sys.argv[2])

# argv[3] - output dir
output_dir = ""
if len(sys.argv) > 3:
   output_dir = sys.argv[3]
output_dir = output_dir.rstrip('/')
'''
if output_dir == "":
   output_dir = "." # default current folder
elif not os.path.isdir(output_dir): # create if not exist
   try:
      os.mkdir(output_dir)
   except:
      util.logMessage("Failed to create folder \"%s\"!" % output_dir)
      util.logMessage("Process terminated.")
      sys.exit(2)
else:
   pass
'''

# argv[4] - process mode
proc_mode = ''
if len(sys.argv) > 4:
   proc_mode = sys.argv[4]
proc_mode = proc_mode.lower()
if not proc_mode == 'cluster':
   proc_mode = 'client'



# update core per job (if it's cluster need more core)
if proc_mode == 'cluster': # cluster mode need one more core
   extra_core_per_job = 1
else:
   extra_core_per_job = 0
core_per_job = core_per_job + extra_core_per_job




# get status JSON
def getStatusJSON():
	js = {}

	resp = requests.get('http://10.26.127.51:8080/json/')
	if resp.status_code != 200:
		# This means something went wrong.
		#raise ApiError('GET /tasks/ {}'.format(resp.status_code))
		pass
	else:
		js = resp.json()

	return js

# get status JSON
def getStatusJSON_mesos():
	js = {}

	#resp = requests.get('http://mesos_master_01:5050/tasks')
	#resp = requests.get('http://mesos_master_01:5050/state')
	#resp = requests.get('http://10.26.126.202:5050/state-summary')
	resp = requests.get('http://mesos_master_01:5050/state-summary')
	if resp.status_code != 200:
		# This means something went wrong.
		#raise ApiError('GET /tasks/ {}'.format(resp.status_code))
		pass
	else:
		js = resp.json()

	return js

# get cores used
def getCoresUsed(statusJSON):

	cores = 8 # default to max used already
	if len(statusJSON) == 0:
		# This means something went wrong.
		pass
	else:
		maxcores = int(statusJSON['cores'])
		cores = int(statusJSON['coresused'])

	return maxcores, cores

# get cores used
def getCoresUsed_mesos(statusJSON):

	maxcores = 8
	cores = 8 # default to max used already
	if len(statusJSON) == 0:
		# This means something went wrong.
		pass
	else:
		maxcores = 0
		cores = 0
		slaves = statusJSON['slaves']
		for slave in slaves:
			maxcores += int(slave['resources']['cpus'])
			cores += int(slave['used_resources']['cpus'])

	return maxcores, cores

# get current job status
def getCurrJobs(statusJSON, prev_jobname):

	numJobs = 0
	numWaitingJobs = 0
	bFoundLastSubmit = False
	if len(statusJSON) == 0:
		return -1, -1, False
	else:
		jobsArr = statusJSON['activeapps']
		numJobs = len(jobsArr)
		for job in jobsArr:
			if job["state"].upper() == 'WAITING':
				numWaitingJobs += 1
			if job["name"] == prev_jobname:
				bFoundLastSubmit = True

	return numJobs, numWaitingJobs, bFoundLastSubmit

# get current job status
def getCurrJobs_mesos(statusJSON, prev_jobname):

	numJobs = 0
	numWaitingJobs = 0
	t_staging = 0
	t_starting = 0
	t_running = 0
	t_killing = 0
	bFoundLastSubmit = False
	if len(statusJSON) == 0:
		return -1, -1, False
	else:
		jobsArr = statusJSON['frameworks']
		for job in jobsArr:
			if (job['name'].upper().find('MARATHON') == -1 and 
				job['name'].upper().find('CHRONOS-') == -1 and
				job['name'].upper().find('SPARK CLUSTER') == -1):
				numJobs += 1
				# further check for waiting task
				if (job['active'] is True and 
					job['TASK_STAGING'] == 0 and
					job['TASK_STARTING'] == 0 and
					job['TASK_RUNNING'] == 0 and
					job['TASK_KILLING'] == 0 and
					job['TASK_FINISHED'] == 0 and
					job['TASK_KILLED'] == 0 and
					job['TASK_FAILED'] == 0 and
					job['TASK_LOST'] == 0 and
					job['TASK_ERROR'] == 0 and
					job['used_resources']['cpus'] == 0):
					numWaitingJobs += 1
			if job['name'] == prev_jobname:
				bFoundLastSubmit = True

		slaves = statusJSON['slaves']
		for worker in slaves:
			t_staging += int(worker["TASK_STAGING"])
			t_starting += int(worker["TASK_STARTING"])
			t_running += int(worker["TASK_RUNNING"])
			t_killing += int(worker["TASK_KILLING"])
		# that should be = numJobs in all slaves so not returning
		numRunningJobs = t_staging + t_starting + t_running + t_killing 

	return numJobs, numWaitingJobs, bFoundLastSubmit

# get current worker status
def haveWorkersResource(statusJSON):

	bWorkerResource = False
	nNoResource = 0
	if len(statusJSON) == 0:
		return bWorkerResource
	else:
		workersArr = statusJSON['workers']
		numWorkers = len(workersArr)
		for worker in workersArr:
			if worker["coresfree"] == 0 or worker["memoryfree"] == 0:
				nNoResource += 1
		if nNoResource == numWorkers:
			bWorkerResource = False
		else:
			bWorkerResource = True

	return bWorkerResource

# get current worker status
def haveWorkersResource_mesos(statusJSON):

	bWorkerResource = False
	nNoResource = 0
	if len(statusJSON) == 0:
		return bWorkerResource
	else:
		slaves = statusJSON['slaves']
		numWorkers = len(slaves)
		for worker in slaves:
			if worker["resources"]["cpus"] == worker["used_resources"]["cpus"] or worker["resources"]["mem"] == worker["used_resources"]["mem"]:
				nNoResource += 1
		if nNoResource == numWorkers:
			bWorkerResource = False
		else:
			bWorkerResource = True

	return bWorkerResource

def canStartNewJob(statusJSON):
	bHaveResource = True
	delay_sec = retry_sec
	global prev_jobname

	# get cores used
	cores_max, cores_used = getCoresUsed_mesos(statusJSON)
 
	# get current job status
	numJobs, numWaitingJobs, bFoundLastSubmit = getCurrJobs_mesos(statusJSON, prev_jobname)
	# reset prev job if found
	if bFoundLastSubmit:
		prev_jobname = ""

	# get current worker resource status
	bHaveWorkersResource = haveWorkersResource_mesos(statusJSON)
	
	# re-calc max num jobs
	max_num_job = int(cores_max / core_per_job)
	if max_num_job > max_num_job_hardlimit: # check against hard limit
		max_num_job = max_num_job_hardlimit



	# case 1: cannot get job info
	if numJobs == -1 or numWaitingJobs == -1:
		bHaveResource = False
		util.logMessage("cannot get jobs info, retry again in %d sec" % delay_sec)
	
	# case 2: last submitted job not show up yet
	elif prev_jobname != "" and not bFoundLastSubmit:
		bHaveResource = False
		delay_sec = check_interval_sec # only wait for little before update
		util.logMessage("last job submit (%s) not completed, retry again in %d sec" % (prev_jobname, delay_sec))

	# case 3: allowed cores exceed
	elif cores_used > (cores_max - core_per_job):
		bHaveResource = False
		util.logMessage("cores exceeding limit, retry again in %d sec" % delay_sec)

	# case 4: more than 1 waiting job
	elif numWaitingJobs > 1:
		bHaveResource = False
		util.logMessage("number of waiting job = %d, retry again in %d sec" % (numWaitingJobs, delay_sec))

	# case 5: max job allowed reached
	elif numJobs >= max_num_job:
		bHaveResource = False
		util.logMessage("reached max num of job (%d/%d), retry again in %d sec" % (numJobs, max_num_job, delay_sec))

	# case 6: all worker occupied - either no avail core or no avail mem on all the workers
	elif bHaveWorkersResource == False:
		bHaveResource = False
		util.logMessage("all workers are occupied, retry again in %d sec" % delay_sec)


	return bHaveResource, delay_sec



def worker():
	"""worker function"""
	global prev_jobname
	jobname = "parse_set_%03d" % (filenum)

	#id = str(uuid.uuid4())
	util.logMessage("Task %s start..." % jobname)

	# submit new job - xml parser
	#exec_str = "spark-submit --master spark://master:7077 --executor-memory 1g --driver-memory 512m --total-executor-cores 2 %s/kpi_parser_umts_eric.py ericsson_umts_demo/set_%03d \"%s\" \"%s\" &" % (curr_py_dir, filenum, jobname, output_dir)
	if proc_mode != 'cluster':
		exec_str = "/opt/spark/bin/spark-submit --master mesos://mesos_master_01:5050 --driver-memory 512m --executor-memory 966m --total-executor-cores 2 %s/kpi_parser_umts_eric.py \"%s\" /mnt/nfs/ttskpiraw/input/umts-eric/set_%03d \"tts@mesos_fs_01|%s\" \"client\" &" % (curr_py_dir, jobname, filenum, output_dir)
	else: # cluster
		exec_str = "/opt/spark/bin/spark-submit --master mesos://mesos_master_01:7077 --deploy-mode cluster --driver-memory 512m --executor-memory 966m --total-executor-cores 2 --py-files \"file:///home/tts/ttskpiraw/code/umts-eric/util.py,file:///home/tts/ttskpiraw/code/umts-eric/xmlparser_umts_eric.py,file:///home/tts/ttskpiraw/code/umts-eric/config.ini\" %s/kpi_parser_umts_eric.py \"%s\" /mnt/nfs/ttskpiraw/input/umts-eric/set_%03d \"tts@mesos_fs_01\|%s\" \"cluster\"" % (curr_py_dir, jobname, filenum, output_dir)

	util.logMessage("%s" % exec_str)

	# update prev jobname
	prev_jobname = jobname

	os.system(exec_str)

	return




#######################################################################################
# main proc ###########################################################################

util.logMessage("multi process started")



check_ctr = 0
while (1):

   # no more file
   if filenum > max_filenum:
      break
     
   try:

      # get status
      #statusJSON = getStatusJSON()
      statusJSON = getStatusJSON_mesos()

      '''
      cores_max, cores_used = getCoresUsed_mesos(statusJSON)
      print 'max:%s, used:%s' % (cores_max, cores_used)

      print 'have resource: %s' % haveWorkersResource_mesos(statusJSON)

      numJobs, numWaitingJobs, bFoundLastSubmit = getCurrJobs_mesos(statusJSON, '1x2c_client')
      print 'numJobs: %s; numWaitingJobs: %s; bFoundLastSubmit: %s' % (numJobs, numWaitingJobs, bFoundLastSubmit)
      exit(0)
      '''

      # get cores used
      cores_max, cores_used = getCoresUsed_mesos(statusJSON)
      util.logMessage("current cores used: %d/%d" % (cores_used, cores_max))


      bStartNewJob, delay_sec = canStartNewJob(statusJSON)
      if bStartNewJob == False:
         check_ctr = 0 # reset counter
         time.sleep(delay_sec)
         continue

      # do last check before adding last available job slot
      if (cores_used == (cores_max - core_per_job)) and (check_ctr < max_check_ctr):
         util.logMessage("cores close to limit, retry again in %d sec" % (check_interval_sec*2))
         check_ctr += 1
         time.sleep(check_interval_sec*2)
         continue
      else: # second time checking - last slot still available, initiate new job
         check_ctr = 0 # reset counter


      worker()

      # wait some sec before next task
      time.sleep(check_interval_sec)

      filenum += 1

   except IOError as e:
      util.logMessage("I/O error({0}): {1}".format(e.errno, e.strerror))
   except ValueError:
      util.logMessage("Could not convert data to an integer.")
   #except:
   #   util.logMessage("Unexpected error: %s" % sys.exc_info()[0])


util.logMessage("multi process ended")
exit(0)
