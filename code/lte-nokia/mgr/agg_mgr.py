#!/usr/bin/python
import os
import sys
import time
import datetime
import uuid
import json
import glob
import shutil # move file

import logging

import requests
import util



## Constants
new_job_delay_sec = 3 # 12 # sec to check inbetween submit of new job
prev_job_wait_delay_sec = 3 # sec to wait for previous job to show up
general_retry_delay_sec = 4 # sec to retry when all cores are busy/used
core_close_to_limit_delay_sec = 6 # sec to wait when tasks almost used up all cores

core_per_job = 4 # core per job
max_check_ctr = 1 # max num of recheck when there is just 1 job slot left
max_num_job = 6 # max num of job allow concurrently
max_num_job_hardlimit = 80 # max num of job (hard limit)



# set some key var
curr_py_path = os.path.realpath(__file__) # current running file - abs path
curr_py_dir, curr_py_filename = os.path.split(curr_py_path)  # current file and folder - abs path
#curr_py_dir = os.path.dirname(curr_py_path)

# get proc time - [0] proc date yyyymmdd; [1] proc time hhmmssiii (last 3 millisec)
procDatetimeArr = datetime.datetime.now().strftime('%Y%m%d %H%M%S%f').split(' ')
procDatetimeArr[1] = procDatetimeArr[1][:-3]

## globals
prev_jobname = ""
check_ctr = 0
exportMode = 1 # 1: save pq + export csv; 2: save pq only; 3: export csv only

# argv[1] - input dir
# argv[2] - output dir - if empty, no export csv
# argv[3] - cell lookup parquet dir
# argv[4] - output parquet dir
# argv[5] - option json string (optional); 
#           default
#            '{
#              "tech" : "lte", 
#              "vendor" : "eric",
#              "zkStr" : "zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos"
#              "master" : "mesos_master_01", 
#              "masterPort" : 5050,
#              "dispatcherPort" : 7077,
#              "newJobDelay" : 3,
#              "prevJobDelay" : 3,
#              "genRetryDelay" : 4,
#              "closeToLimitDelay" : 6,
#              "exec_core_per_job" : 4,
#              "drvr_mem" : "512m",
#              "exec_mem" : "2g",
#              "logfile" : "" - empty = no log file
#             }'
##           "":null --> None in python (no coalesce)
##           "":false/true --> False/True in python
# argv[6] - (optional) "cluster" or "client" mode


if len(sys.argv) < 6:
   util.logMessage("Error: param incorrect.")
   sys.exit(2)

# argv[5] - option json - get first to get all options
optionJSONStr = ""
optionJSON = ""
if len(sys.argv) > 5:
   optionJSONStr = sys.argv[5]
   optionJSON = sys.argv[5]
if optionJSON == "":
   optionJSON = '{"master":"", "masterPort":5050}'
try:
   optionJSON = json.loads(optionJSON)
except Exception as e: # error parsing json
   optionJSON = '{"master":"", "masterPort":5050}'
   optionJSON = json.loads(optionJSON) 

# default val if not exist
if 'tech' not in optionJSON:
   optionJSON[u'tech'] = "lte"
optionJSON[u'tech'] = optionJSON[u'tech'].lower()   
optionJSON[u'techUp'] = optionJSON[u'tech'].upper()   
if 'vendor' not in optionJSON:
   optionJSON[u'vendor'] = "eric"
optionJSON[u'vendor'] = optionJSON[u'vendor'].lower()   
optionJSON[u'vendorUp'] = optionJSON[u'vendor'].upper()   
if optionJSON[u'vendorUp'] == 'ERIC':
   optionJSON[u'vendorFULL'] = 'ERICSSON'
else:
   optionJSON[u'vendorFULL'] = 'NOKIA'
if 'zkStr' not in optionJSON:
   optionJSON[u'zkStr'] = "zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos"
if 'master' not in optionJSON:
   optionJSON[u'master'] = ""
if 'masterPort' not in optionJSON:
   optionJSON[u'masterPort'] = 5050
if 'dispatcherPort' not in optionJSON:
   optionJSON[u'dispatcherPort'] = 7077
if 'newJobDelay' not in optionJSON:
   optionJSON[u'newJobDelay'] = 3
if 'prevJobDelay' not in optionJSON:
   optionJSON[u'prevJobDelay'] = 3
if 'genRetryDelay' not in optionJSON:
   optionJSON[u'genRetryDelay'] = 4
if 'closeToLimitDelay' not in optionJSON:
   optionJSON[u'closeToLimitDelay'] = 6
if 'exec_core_per_job' not in optionJSON:
   optionJSON[u'exec_core_per_job'] = 4
if 'drvr_mem' not in optionJSON:
   optionJSON[u'drvr_mem'] = "512m"
if 'exec_mem' not in optionJSON:
   if optionJSON[u'vendor'] == 'nokia': # nokia need 512m, eric need 966m
      optionJSON[u'exec_mem'] = "2g"
   else:
      optionJSON[u'exec_mem'] = "2g"
if 'logfile' not in optionJSON:
   optionJSON[u'logfile'] = ""

# init logger
util.loggerSetup(__name__, optionJSON[u'logfile'], logging.DEBUG)






# create lock
lockpath = '/tmp/agg_mgr_%s_%s.lock' % (optionJSON[u'vendor'], optionJSON[u'tech'])
try:
   os.makedirs(lockpath)
   util.logMessage("Created lock %s" % lockpath)
except OSError:
   if sys.argv[1] == '': # export only, ignore lock
      util.logMessage("Found existing lock %s, but continue process (export only)." % lockpath)
   else: # input dir not empty, save pq, need lock
      util.logMessage("Found existing lock %s, quit process." % lockpath)
      sys.exit(0)





# argv[1] - input dir
input_dir = sys.argv[1]
input_dir = input_dir.rstrip('/')
if input_dir == '':
   exportMode = 3 # csv only
if exportMode != 3 and not os.path.isdir(input_dir): # not only export csv
   util.logMessage("Failed to open input location \"%s\"!" % input_dir)
   util.logMessage("Process terminated.")
   util.endProcess(lockpath, 2)

# argv[2] - output dir
output_dir = sys.argv[2]
output_dir = output_dir.rstrip('/')
# new logic: if not export mode 3, reset output_dir to empty,
# because now only support mode 2 (save pq only) and mode 3 (export csv only)
# not support mode 1 (save pq and export csv) anymore since current logic doesn't allow
if exportMode != 3 and output_dir != '':
   util.logMessage("Mode 1 (pq + csv) not supported, default to mode 2 - output dir \"%s\" not used." % output_dir)
   output_dir = ''
if output_dir == '':
   exportMode = 2 # pq only
if exportMode != 2 and not os.path.isdir(output_dir): # not only save pq
   util.logMessage("Failed to open output location \"%s\"!" % output_dir)
   util.logMessage("Process terminated.")
   util.endProcess(lockpath, 2)

# safeguard - check if there are files to process for mode 1 or 2 (if none, mode 1 will become 3 - export csv only)
if exportMode != 3: # not only export csv
   check_path = input_dir+"/ttskpiraw_%s_%s_*_TMO*.tgz" % (optionJSON[u'vendorFULL'], optionJSON[u'techUp'])
   if len(glob.glob(check_path)) <= 0:  # no file
      if exportMode == 2: # if no file and mode 2, error out
         util.logMessage("No parser output to process: %s" % check_path)
         util.endProcess(lockpath, 0)
      else: # if no file and mode 1, changed to mode 3
         exportMode = 3
   else: # if have file, keep mode 1
      pass

# create staging (if not exist)
if exportMode == 3: # only export csv, no need input dir
   staging_dir = 'placeholder/staging'
else: # mode 1 or 2 need to create staging
   staging_dir = input_dir+'/staging'
if exportMode != 3 and not os.path.isdir(staging_dir): # create if not exist
   try:
      os.mkdir(staging_dir)
   except:
      util.logMessage("Failed to create folder \"%s\"!" % staging_dir)
      util.logMessage("Process terminated.")
      util.endProcess(lockpath, 2)

# create secondary staging
staging_dir_sub = staging_dir + "/ttskpiagg_%s_%s_%s_%s_TMO" % (optionJSON[u'vendorFULL'], optionJSON[u'techUp'], procDatetimeArr[0], procDatetimeArr[1])
if exportMode != 3 and not os.path.isdir(staging_dir_sub): # create if not exist
   try:
      os.mkdir(staging_dir_sub)
   except:
      util.logMessage("Failed to create folder \"%s\"!" % staging_dir_sub)
      util.logMessage("Process terminated.")
      util.endProcess(lockpath, 2)

# argv[3] - cell lookup parquet dir
input_celllookup_parq = sys.argv[3]
input_celllookup_parq = input_celllookup_parq.rstrip('/')
if not os.path.isdir(input_celllookup_parq):
   util.logMessage("Failed to open cell lookup parquet location \"%s\"!" % input_celllookup_parq)
   util.logMessage("Process terminated.")
   util.endProcess(lockpath, 2)

# argv[4] - output parquet dir
output_parq = sys.argv[4]
output_parq = output_parq.rstrip('/')
if not os.path.isdir(output_parq): # create if not exist
   try:
      os.mkdir(output_parq)
   except:
      util.logMessage("Failed to create output parquet location \"%s\"!" % output_parq)
      util.logMessage("Process terminated.")
      util.endProcess(lockpath, 2)

# argv[5] - option json - done in the beginning...
# set new delay variables
new_job_delay_sec = optionJSON[u'newJobDelay'] 
prev_job_wait_delay_sec = optionJSON[u'prevJobDelay']
general_retry_delay_sec = optionJSON[u'genRetryDelay']
core_close_to_limit_delay_sec = optionJSON[u'closeToLimitDelay']

# argv[6] - process mode
proc_mode = ''
if len(sys.argv) > 6:
   proc_mode = sys.argv[6]
proc_mode = proc_mode.lower()
if not proc_mode == 'cluster':
   proc_mode = 'client'


# update core per job (if it's cluster need more core)
core_per_job = optionJSON[u'exec_core_per_job']
if proc_mode == 'cluster': # cluster mode need one more core
   extra_core_per_job = 1
else:
   extra_core_per_job = 0
core_per_job = core_per_job + extra_core_per_job






# update master info
# logic: if master provided, ignore zkStr and set master
#        else if zkStr provided, use it to find master
#        else if zkStr empty, use default zkStr (zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos) to find master
#        if still cannot find master, use default (mesos_master_01)
def updateMasterInfo():
	global optionJSON

	if optionJSON[u'master'] != '': # if master defined, not using zookeeper
	   optionJSON[u'zkStr'] = ''
	   util.logMessage("Master default at %s:%d" % (optionJSON[u'master'], optionJSON[u'masterPort']))
	else: # if master not defined, use zookeeper
	   if optionJSON[u'zkStr'] != '':
	      util.logMessage("Try to determine master using zookeeper string: %s" % optionJSON[u'zkStr'])
	      master, masterPort = util.getMesosMaster(optionJSON[u'zkStr'])
	   else:
	      util.logMessage("Try to determine master using default zookeeper string: %s" % 
			"zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos")
	      master, masterPort = util.getMesosMaster()
	   if master == '': # master not found through zookeeper
	      optionJSON[u'master'] = "mesos_master_01"
	      util.logMessage("Cannot get master from zookeeper; master default at %s:%d" % (optionJSON[u'master'], optionJSON[u'masterPort']))
	   else: # master found through zookeeper
	      optionJSON[u'master'] = master
	      optionJSON[u'masterPort'] = masterPort
	      util.logMessage("Master detected at %s:%d" % (optionJSON[u'master'], optionJSON[u'masterPort']))

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
	resp = requests.get("http://%s:%d/master/state-summary" % (optionJSON[u'master'], optionJSON[u'masterPort']))
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
def getCurrJobs(statusJSON):

	global prev_jobname

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
				prev_jobname = "" # reset prev job if found

	return numJobs, numWaitingJobs, bFoundLastSubmit

# get current job status
def getCurrJobs_mesos(statusJSON):

	global prev_jobname

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
				prev_jobname = "" # reset prev job if found

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
	delay_sec = general_retry_delay_sec # general retry delay
	global prev_jobname
	global check_ctr

	# get status
	statusJSON = getStatusJSON_mesos()

	# get cores used
	cores_max, cores_used = getCoresUsed_mesos(statusJSON)
	util.logMessage("Current cores used: %d/%d" % (cores_used, cores_max))
 
	# get current job status
	numJobs, numWaitingJobs, bFoundLastSubmit = getCurrJobs_mesos(statusJSON)

	# get current worker resource status
	bHaveWorkersResource = haveWorkersResource_mesos(statusJSON)
	
	# re-calc max num jobs
	max_num_job = int(cores_max / core_per_job)
	if max_num_job > max_num_job_hardlimit: # check against hard limit
		max_num_job = max_num_job_hardlimit



	# case 1: cannot get job info
	if numJobs == -1 or numWaitingJobs == -1:
		bHaveResource = False
		check_ctr = 0 # reset retry counter
		util.logMessage("cannot get jobs info, retry again in %d sec" % delay_sec)

		'''
		# turn off to relax the check so we not neccessary wait for job sumbit finish
	# case 2: last submitted job not show up yet
	elif prev_jobname != "" and not bFoundLastSubmit:
		bHaveResource = False
		delay_sec = prev_job_wait_delay_sec # only wait for little before update
		util.logMessage("last job submit: %s not completed, retry again in %d sec" % (prev_jobname, delay_sec))
		'''

	# case 3: allowed cores exceed
	elif cores_used > (cores_max - core_per_job):
		bHaveResource = False
		check_ctr = 0 # reset retry counter
		util.logMessage("cores exceeding limit, retry again in %d sec" % delay_sec)

	# case 4: do last n # of check before adding last available job slot
	# check_ctr == max_check_ctr means already check n # of times, pass test
	elif cores_used == (cores_max - core_per_job):
		if check_ctr < max_check_ctr:
			check_ctr += 1
			bHaveResource = False
			delay_sec = core_close_to_limit_delay_sec
			util.logMessage("cores close to limit, retry again in %d sec" % (delay_sec))
		else:
			check_ctr = 0 # condition met, reset retry counter

	# case 5: more than 1 waiting job
	elif numWaitingJobs > 1:
		bHaveResource = False
		check_ctr = 0 # reset retry counter
		util.logMessage("number of waiting job = %d, retry again in %d sec" % (numWaitingJobs, delay_sec))

		'''
		# cannot check this as now there are other different jobs in the pool
	# case 6: max job allowed reached
	elif numJobs >= max_num_job:
		bHaveResource = False
		check_ctr = 0 # reset retry counter
		util.logMessage("reached max num of job (%d/%d), retry again in %d sec" % (numJobs, max_num_job, delay_sec))
		'''

	# case 7: all worker occupied - either no avail core or no avail mem on all the workers
	elif bHaveWorkersResource == False:
		bHaveResource = False
		check_ctr = 0 # reset retry counter
		util.logMessage("all workers are occupied, retry again in %d sec" % delay_sec)



	return bHaveResource, delay_sec

def getAvailablePort(index = 0, exportOnly = False):

    baseport = 30000
    portrange = 1250
    portshift = 1250/16
    if exportOnly:
        baseport = 31250

    startport = baseport + portshift * (index)
    if startport > baseport + portrange:
        startport = baseport

    port = startport

    # disable listening port check
    '''
    port = -1
    for p in range(startport, startport+portrange+1):
        if not isPortListening(p):
            port = p
            break
    '''
    return port

# worker function
def worker(seqfile, index):

	global prev_jobname
	seqfile_dir, seqfile_file = os.path.split(seqfile)
	jobname = 'stg3_' + seqfile_file
	jobname = jobname.replace(' ', '-') # for cluster mode, job name should not contain space - spark bug

	util.logMessage("Task %s start..." % jobname)

	# create master string
	if proc_mode == 'cluster': # assume the leading master that zk return is the one to be use for dispatcher
		exec_str_master = "mesos://%s:%d" % (optionJSON[u'master'], optionJSON[u'dispatcherPort'])
	else: # client
		if optionJSON[u'zkStr'] != '':
			exec_str_master = "mesos://%s" % (optionJSON[u'zkStr'])
		else:
			exec_str_master = "mesos://%s:%d" % (optionJSON[u'master'], optionJSON[u'masterPort'])

	exportOnly = False
	if exportMode == 3:
		exportOnly = True
	port = getAvailablePort(index, exportOnly)

	# create spark string
	exec_str_spark = "/opt/spark/bin/spark-submit \
--conf spark.ui.port=%d \
--conf spark.network.timeout=900s \
--conf spark.rpc.askTimeout=900s \
--conf spark.executor.heartbeatInterval=900s \
--conf 'spark.driver.extraJavaOptions=-XX:ParallelGCThreads=2' \
--conf 'spark.executor.extraJavaOptions=-XX:ParallelGCThreads=2' \
--master %s \
--deploy-mode %s \
--driver-memory %s \
--executor-memory %s \
--total-executor-cores %d" % (
		port,
		exec_str_master,
		proc_mode,
		optionJSON[u'drvr_mem'],
		optionJSON[u'exec_mem'],
		optionJSON[u'exec_core_per_job'])
	if proc_mode == 'cluster': # cluster have more options to be set
		exec_str_spark += " --py-files \"%s,%s,%s\"" % (
			"file://%s/../util.py" % curr_py_dir,
			"file://%s/../schema/%s_%s_cell_avail_schema.json" % (curr_py_dir, optionJSON[u'tech'], optionJSON[u'vendor']),
			"file://%s/../sql/%s_%s_sql.json" % (curr_py_dir, optionJSON[u'tech'], optionJSON[u'vendor']))

	# create python string
	exec_str_py = "%s/../%s_%s_aggregator.py" % (curr_py_dir, optionJSON[u'tech'], optionJSON[u'vendor'])
	if exportMode == 3: # mode 3 - export csv only
		exec_str_app = "%s \
3 \
%s \
%s \
TMO \
\"%s\" \
\"%s\" \
\"%s\" \
'%s'" % (exec_str_py, 
		optionJSON[u'vendorUp'],
		optionJSON[u'techUp'],
		output_parq,
		output_dir,
		input_celllookup_parq,
		optionJSONStr)
	elif exportMode == 2: # mode 2 - create parquet only
		exec_str_app = "%s \
2 \
%s \
%s \
TMO \
\"%s\" \
\"%s/*.txt\" \
\"%s\" \
\"%s\" \
'%s'" % (exec_str_py, 
		optionJSON[u'vendorUp'],
		optionJSON[u'techUp'],
		input_dir,
		seqfile, 
		input_celllookup_parq,
		output_parq,
		optionJSONStr)
	else: # mode 1 - create parquet and export csv - not support anymore, should not run to here
		exec_str_app = "%s \
1 \
%s \
%s \
TMO \
\"%s\" \
\"%s/*.txt\" \
\"%s\" \
\"%s\" \
\"%s\" \
'%s'" % (exec_str_py, 
		optionJSON[u'vendorUp'],
		optionJSON[u'techUp'],
		input_dir,
		seqfile, 
		input_celllookup_parq,
		output_parq,
		output_dir,
		optionJSONStr)
	if proc_mode != 'cluster': # client - support multi master (zookeeper)
		exec_str_app += " &" 
	else: # cluster - currently not support multi master (zookeeper)
		pass

	exec_str = exec_str_spark + " " + exec_str_app

	'''
	# old samples
	# submit new job - xml parser
	#exec_str = "spark-submit --master spark://master:7077 --executor-memory 512m --driver-memory 512m --total-executor-cores 2 %s/kpi_parser_eric.py \"%s\" %s \"%s\" &" % (curr_py_dir, jobname, seqfile, output_dir)
	if proc_mode != 'cluster': # client - support multi master (zookeeper)
	#	exec_str = "/opt/spark/bin/spark-submit --master mesos://mesos_master_01:5050 --driver-memory 512m --executor-memory 966m --total-executor-cores 2 %s/kpi_parser_lte_eric.py \"%s\" %s \"tts@mesos_fs_01|%s\" \"client\" &" % (curr_py_dir, jobname, seqfile, output_dir)
		exec_str = "/opt/spark/bin/spark-submit --master mesos://zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos --driver-memory 512m --executor-memory 966m --total-executor-cores 2 %s/kpi_parser_lte_eric.py \"%s\" %s \"imnosrf@mesos_fs_01|%s\" \"client\" &" % (curr_py_dir, jobname, seqfile, output_dir)
	else: # cluster - currently not support multi master (zookeeper)
	#	exec_str = "/opt/spark/bin/spark-submit --master mesos://mesos_master_01:7077 --deploy-mode cluster --driver-memory 512m --executor-memory 966m --total-executor-cores 2 --py-files \"file:///home/tts/ttskpiraw/code/lte-eric/util.py,file:///home/tts/ttskpiraw/code/lte-eric/xmlparser_lte_eric.py,file:///home/tts/ttskpiraw/code/lte-eric/config.ini\" %s/kpi_parser_lte_eric.py \"%s\" %s \"tts@mesos_fs_01\|%s\" \"cluster\"" % (curr_py_dir, jobname, seqfile, output_dir)
		exec_str = "/opt/spark/bin/spark-submit --master mesos://mesos_master_01:7077 --deploy-mode cluster --driver-memory 512m --executor-memory 966m --total-executor-cores 2 --py-files \"file:///home/imnosrf/ttskpiraw/code/lte-eric/util.py,file:///home/imnosrf/ttskpiraw/code/lte-eric/xmlparser_lte_eric.py,file:///home/imnosrf/ttskpiraw/code/lte-eric/config.ini\" %s/kpi_parser_lte_eric.py \"%s\" %s \"imnosrf@mesos_fs_01\|%s\" \"cluster\"" % (curr_py_dir, jobname, seqfile, output_dir)
	'''

	util.logMessage("%s" % exec_str)

	# update prev jobname
	prev_jobname = jobname

	os.system(exec_str)






#######################################################################################
# main proc ###########################################################################
def main(input_dir, optionJSON):

   '''
   # sameple code
   # get status
   statusJSON = getStatusJSON_mesos()
   cores_max, cores_used = getCoresUsed_mesos(statusJSON)
   print 'max:%s, used:%s' % (cores_max, cores_used)
   print 'have resource: %s' % haveWorkersResource_mesos(statusJSON)
   numJobs, numWaitingJobs, bFoundLastSubmit = getCurrJobs_mesos(statusJSON, '1x2c_client')
   print 'numJobs: %s; numWaitingJobs: %s; bFoundLastSubmit: %s' % (numJobs, numWaitingJobs, bFoundLastSubmit)
   exit(0)
   '''

   global exportMode

   if exportMode != 3: # not only export csv
      # go thru all seq file/folder
      inputSeqPath = input_dir+"/ttskpiraw_%s_%s_*_TMO*.tgz" % (optionJSON[u'vendorFULL'], optionJSON[u'techUp'])
      inputSeqList = glob.glob(inputSeqPath)
      if len(inputSeqList) <= 0:  # no file
         util.logMessage("No parser output to process: %s" % inputSeqPath)
         os.system("rm -rf '%s'" % staging_dir_sub) # remove staging sub folder (since will not be removed by proc)
         if exportMode == 2: # if save pq only (no output), and also no input, end process
            util.endProcess(lockpath, 0)
         else: # if no input, but have output, only do export
            exportMode = 3


   # export only mode
   if exportMode == 3:
      # submit one process to work on the whole folder (of multiple txt file)
      try:
         # get status
         statusJSON = getStatusJSON_mesos()
         bStartNewJob, delay_sec = canStartNewJob(statusJSON)
         while (bStartNewJob == False):
            time.sleep(delay_sec)
            bStartNewJob, delay_sec = canStartNewJob(statusJSON) # retest after the sleep

         # process file
         worker(staging_dir_sub, 0)

      except Exception as e:
         util.logMessage("Error: failed to export file %s\n%s" % (staging_dir_sub, e))
      except:
         util.logMessage("Unexpected error")

      return 0


   # move seq file into staging_sub first to prevent other proc from touching them
   inputSeqStageList = []
   for curr_file in sorted(inputSeqList):
      util.logMessage("Moving %s to staging dir %s" % (curr_file, staging_dir_sub))
      try:
         shutil.move(curr_file, staging_dir_sub)
         curr_filedir, curr_filename = os.path.split(curr_file)
         inputSeqStageList.append(os.path.join(staging_dir_sub,curr_filename))
      except shutil.Error as e:
         util.logMessage("Error: failed to move file %s\n%s" % (curr_file, e))
      except:
         util.logMessage("Unexpected error")


   # going to each file in the staging area and unzip into one folder
   for curr_file in inputSeqStageList:
      try:

         exec_str = ''
         if optionJSON[u'vendor'] == 'eric':
            exec_str = "tar -xvzf %s -C %s *%s_%s*TMO.txt" % (curr_file, staging_dir_sub, optionJSON[u'vendorFULL'], optionJSON[u'techUp'])
         else: # nokia
            exec_str = "tar -xvzf %s -C %s *%s_%s*TMO*.txt" % (curr_file, staging_dir_sub, optionJSON[u'vendorFULL'], optionJSON[u'techUp'])
         util.logMessage('unzipping files: %s' % exec_str)
         os.system(exec_str)

      except Exception as e:
         util.logMessage("Error: failed to process file %s\n%s" % (curr_file, e))
         # try to move it back to input dir for re-processing next round
         try:
            shutil.move(curr_file, input_dir)
         except shutil.Error as e:
            util.logMessage("Error: failed to move file %s\n%s" % (curr_file, e))
         except:
            util.logMessage("Unexpected error")
      except:
         util.logMessage("Unexpected error")
         # try to move it back to input dir for re-processing next round
         try:
            shutil.move(curr_file, input_dir)
         except shutil.Error as e:
            util.logMessage("Error: failed to move file %s\n%s" % (curr_file, e))
         except:
            util.logMessage("Unexpected error")


   # move files into sub folders by file type
   filetypeArr = []
   filetypeDirArr = []  
   stagingFileList = glob.glob(staging_dir_sub+"/*.txt")
   if len(stagingFileList) > 0:  # safeguard
      for curr_file in stagingFileList:
         curr_stg_dir, curr_data_filename = os.path.split(curr_file)
         filenameArr = curr_data_filename.split('.')[0].split('_')
         filetype = '_'.join(filenameArr[6:])
         filetypeDir = staging_dir_sub + '/' + filetype

         if filetype not in filetypeArr: # create new dir

            filetypeArr.append(filetype)        

            if not os.path.isdir(filetypeDir): # create if not exist
               try:
                  os.mkdir(filetypeDir)
                  filetypeDirArr.append(filetypeDir)
               except:
                  util.logMessage("Failed to create folder \"%s\"!" % filetypeDir)
                  util.logMessage("Process terminated.")
                  util.endProcess(lockpath, 2)            
        
         # move file by filetype
         try:
            shutil.move(curr_file, filetypeDir)
         except shutil.Error as e:
            util.logMessage("Error: failed to move file %s\n%s" % (curr_file, e))
         except:
            util.logMessage("Unexpected error")


   # going to each file type folder in the staging area and submit process
   index = 0
   for curr_dir in filetypeDirArr:
      try:

         # get status
         statusJSON = getStatusJSON_mesos()
         bStartNewJob, delay_sec = canStartNewJob(statusJSON)
         while (bStartNewJob == False):
            time.sleep(delay_sec)
            bStartNewJob, delay_sec = canStartNewJob(statusJSON) # retest after the sleep

         # process file
         worker(curr_dir, index)
         index+=1

         # wait some sec before next task
         time.sleep(new_job_delay_sec)

      except Exception as e:
         util.logMessage("Error: failed to process file %s\n%s" % (curr_file, e))
         # WES_TEST: doesn't work like that
         # try to move it back to input dir for re-processing next round
         try:
            shutil.move(curr_file, input_dir)
         except shutil.Error as e:
            util.logMessage("Error: failed to move file %s\n%s" % (curr_file, e))
         except:
            util.logMessage("Unexpected error")
      except:
         util.logMessage("Unexpected error")
         # try to move it back to input dir for re-processing next round
         try:
            shutil.move(curr_file, input_dir)
         except shutil.Error as e:
            util.logMessage("Error: failed to move file %s\n%s" % (curr_file, e))
         except:
            util.logMessage("Unexpected error")


   return 0






if __name__ == "__main__":

   # Execute Main functionality
   updateMasterInfo() # update master from zkStr
   util.logMessage("multi process started with option:\n%s" % json.dumps(optionJSON, sort_keys=True, indent=3)) # pretty print option JSON
   ret = main(input_dir, optionJSON)
   util.logMessage("multi process ended")
   util.endProcess(lockpath, ret)


