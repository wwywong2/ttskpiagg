#!/usr/bin/python

## Imports
import sys
import os
import time
import uuid
import json
import socket

import util

# spark main
from pyspark import SparkConf, SparkContext
from pyspark import SparkFiles

# spark sql
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from operator import add



## Constants
socket_retry_sec = 5 # num of sec to wait for next retry when socket fail
socket_retry_num = 10 # num of times to retry when socket fail



# set some key var
curr_py_path = os.path.realpath(__file__) # current running file - abs path
curr_py_dir, curr_py_filename = os.path.split(curr_py_path)  # current file and folder - abs path

# argv[1] - process name
# argv[2] - input file (csv)
# argv[3] - output parquet dir
# argv[4] - process mode: 'client' or 'cluster'

APP_NAME = "kpiAggrApp"
# argv[1] - take app name from param
if len(sys.argv) > 1:
   APP_NAME = sys.argv[1]


# argv[3] - output dir
output_dir = ""
if len(sys.argv) > 3:
   output_dir = sys.argv[3]
output_dir = output_dir.rstrip('/')
#output_dir = curr_py_dir+'/output_'+time.strftime("%Y%m%d%H%M%S")

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



# argv[4] - process mode
proc_mode = ''
if len(sys.argv) > 4:
   proc_mode = sys.argv[4]
proc_mode = proc_mode.lower()
if not proc_mode == 'cluster':
   proc_mode = 'client'





##OTHER FUNCTIONS/CLASSES

def csvToDF(spark, csvFile, schema=None):

   try:
      if schema is None:
         df = spark.read.csv(filename,
                             ignoreLeadingWhiteSpace=True,
                             ignoreTrailingWhiteSpace=True,
                             header=True,
                             timestampFormat='yyyy-MM-dd HH:mm')
      else:
         df = spark.read.csv(filename,
                             ignoreLeadingWhiteSpace=True,
                             ignoreTrailingWhiteSpace=True,
                             header=True,
                             timestampFormat='yyyy-MM-dd HH:mm',
                             schema=schema)

      return df

   except Exception as e:
      util.logMessage("Job: %s: Exception Error: %s!" % (APP_NAME, e))
      return None

   except:
      util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME)
      return None


def jsonToSchema(jsonFile):

   schema = None   

   try:
      with open(jsonFile) as json_data:
         schemaJson = json.load(json_data)
         #print [item for item in schemaJson]

   except Exception as e:
      util.logMessage("Job: %s: Exception Error: %s!" % (APP_NAME, e))
      return None

   except:
      util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME)
      return None


   schema = StructType([StructField.fromJson(item) for item in schemaJson])
   return schema





'''
def f_map(filetuple):
   [fn,bw] = filetuple

   try:
      xml = XMLParser(fn,'bytes','file',bw,SparkFiles.get('config.ini'))
      #util.logMessage("inside map func, after XMLParser(), before xml.ProcessXML()")
      return xml.ProcessXML()
   except Exception as e:
      util.logMessage("err in file: %s" % fn)
      return ""

def f_reduce(a, b):
   #util.logMessage("inside reduce func")
   return a + b
'''





def main(spark,filename,outfilename):


   try:

      sc = spark.sparkContext

      if proc_mode == 'client':
         # add file
         util.logMessage("addFile: %s" % curr_py_dir+'/umts_eric_schema.json')
         sc.addFile(curr_py_dir+'/umts_eric_schema.json')

         # add py reference
         util.logMessage("addPyFile: %s" % curr_py_dir+'/util.py')
         sc.addPyFile(curr_py_dir+'/util.py')




      util.logMessage("process start...")


      # get schema
      schemaFile = "umts_eric_schema.json"
      schema = jsonToSchema(schemaFile)
      util.logMessage("acquired schema from file: %s" % schemaFile)


      # read csv file into dataframe
      util.logMessage("reading file: %s" % filename)
      df = csvToDF(spark, filename, schema)
      if df is None:
         util.logMessage("issue(s) reading file: %s" % filename)
      else:         
         util.logMessage("finish reading file: %s" % filename)

         # show dtypes
         #util.logMessage("dtypes: %s" % df.dtypes)
         # show schema
         #df.printSchema()

         pqf = spark.read.parquet('/mnt/nfs/test/eric_new3.pqz')
         pqf.show(30, truncate=False)
         pqf.createOrReplaceTempView('eric')
         sqlDF = spark.sql("SELECT distinct HL_MARKET FROM eric")
         #sqlDF.coalesce(1).write.parquet('/mnt/nfs/test/eric_new4.pqz', compression='gzip', mode='overwrite')
         sqlDF.show()

         time.sleep(120)





      # stop spark session (and context)
      spark.stop()


      util.logMessage("finish process successfully.")

      return 0





   except Exception as e:

      #util.logMessage('Cleanup location \'%s\'' % output_dir)
      #os.system("rm -rf \'%s\'" % output_dir) 
      util.logMessage("Job: %s: Exception Error: %s!" % (APP_NAME, e))
      raise

   except:

      #util.logMessage('Cleanup location \'%s\'' % output_dir)
      #os.system("rm -rf \'%s\'" % output_dir) 
      util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME)
      raise # not the error we are looking for



   '''
   curr_try_num = 0
   bSocketConnFail = True # init to True so it will go into loop
   while (curr_try_num < socket_retry_num and bSocketConnFail):

      try:

         # map
         #util.logMessage("starting map")
         mapRDD = textRDD.map(f_map)
         #util.logMessage("after map, starting reduce")

         #print mapRDD.count()
         #print mapRDD.collect()


         # reduce
         redRDD = mapRDD.reduce(f_reduce)
         #util.logMessage("after reduce")

         #print redRDD.count()
         #print redRDD.collect()


      except socket.error as e:

         util.logMessage("Job: %s: Socket Error: %s!" % (APP_NAME, e))
         curr_try_num += 1 # increment retry count
         if curr_try_num < socket_retry_num:
            util.logMessage("Job: %s: will retry in %d sec" % (APP_NAME, socket_retry_sec))
            time.sleep(socket_retry_sec)
         else:
            util.logMessage("Job: %s: too many retry (%d)! Give up!" % (APP_NAME, socket_retry_num))

      except socket.timeout as e:

         util.logMessage("Job: %s: Socket Timeout: %s!" % (APP_NAME, e))
         curr_try_num += 1 # increment retry count
         if curr_try_num < socket_retry_num:
            util.logMessage("Job: %s: will retry in %d sec" % (APP_NAME, socket_retry_sec))
            time.sleep(socket_retry_sec)
         else:
            util.logMessage("Job: %s: too many retry (%d)! Give up!" % (APP_NAME, socket_retry_num))

      except Exception as e: # trying to catch could not open socket

         if hasattr(e, 'message') and e.message == 'could not open socket':

            util.logMessage("Job: %s: Socket Error: %s!" % (APP_NAME, e))
            curr_try_num += 1 # increment retry count
            if curr_try_num < socket_retry_num:
               util.logMessage("Job: %s: will retry in %d sec" % (APP_NAME, socket_retry_sec))
               time.sleep(socket_retry_sec)
            else:
               util.logMessage("Job: %s: too many retry (%d)! Give up!" % (APP_NAME, socket_retry_num))

         else:

            util.logMessage('Cleanup location \'%s\'' % output_dir)
            os.system("rm -rf \'%s\'" % output_dir) 
            util.logMessage("Job: %s: Other Exception Error: %s!" % (APP_NAME, e))
            raise # not the error we are looking for

      except:

         util.logMessage('Cleanup location \'%s\'' % output_dir)
         os.system("rm -rf \'%s\'" % output_dir) 
         util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME)
         raise # not the error we are looking for

      else:

         bSocketConnFail = False




   if bSocketConnFail: # max retry reached, still fail
      util.logMessage("socket issue(s), failed parsing job: %s" % APP_NAME)
      util.logMessage('Cleanup location \'%s\'' % output_dir)
      os.system("rm -rf \'%s\'" % output_dir) 
      return 1
   '''



   # print to file
   input_dir, input_filename = os.path.split(filename + '.txt')  # current file and folder - abs path
   input_dir, output_gz_file = os.path.split(filename + '_' + time.strftime("%Y%m%d%H%M%S") + '.tgz')
   output_filename = output_dir + '/' + input_filename
   err_filename = output_filename.split('.txt')[0] + '.error.txt'

   util.logMessage("start writing file: %s" % output_filename)

   err = ""
   rslt = ""
   with open(output_filename,'w') as fout:
      uuidstr = str(uuid.uuid4())
      mycache = list()
      header = XMLParser.GetHeader(curr_py_dir+'/config.ini')
      fout.write(header+'\n')
      [rslt, err] = XMLParser.GetReport(redRDD, mycache)
      for row in rslt.split("\n"):
         if len(row) > 1:
            fout.write(uuidstr+','+row+'\n')

   # output error log
   with open(err_filename,'w') as ferr:
      ferr.write(err)


   util.logMessage("finish writing file: %s" % output_filename)


   # stop spark session (and context)
   spark.stop()

   try:

      if proc_mode == 'cluster':
         # copy std logs into output      
         util.logMessage('Copying logs')
         sys.stdout.flush()
         sys.stderr.flush()
         os.system("cp std* \'%s\'" % output_dir)


      # zip folder into file
      output_gz_path = curr_py_dir+'/'+output_gz_file
      util.logMessage('Zipping files: cd %s && tar -cvzf %s *' % (output_dir, output_gz_path))
      os.system("cd %s && tar -cvzf %s *" % (output_dir, output_gz_path))


      # copy file to external location
      # method 1 (cannot take '*'): recursive: .../output --> .../result/output/*
      '''
      util.logMessage('Copying to remote location @ %s: %s' % (outfile_addr, outfile_path))
      ret = util.copyFileToRemote1(outfile_addr, outfile_user, 'tts1234', output_dir, outfile_path)
      if not ret['ret']:
         #util.logMessage('ret: %s' % ret) # cmd, ret, retCode
         util.logMessage('Copy to remote location failed: %s - Error Code: %s' % (outfile_path, ret['retCode']))
         sys.exit(1)
      '''

      # method 2 (take '*'): recursive: .../output/* --> .../result/*
      util.logMessage('Copying to remote location @ %s: %s' % (outfile_addr, outfile_path))
      #ret = util.copyFileToRemote2(outfile_addr, outfile_user, 'tts1234', output_dir+'/*', outfile_path)
      ret = util.copyFileToRemote2(outfile_addr, outfile_user, 'tts1234', output_gz_path, outfile_path)
      if not ret['ret']:
         #util.logMessage('ret: %s' % ret) # cmd, ret, retCode, errmsg, outmsg
         util.logMessage('Copy to remote location failed: %s - Error Code: %s' % (outfile_path, ret['retCode']))
         util.logMessage('Error Msg: %s' % ret['errmsg'])
         #sys.exit(1)
         return ret['retCode']

      util.logMessage('Finished copying to remote location @ %s: %s' % (outfile_addr, outfile_path))

   
   except Exception as e:

      util.logMessage("Job: %s: Exception Error: %s!" % (APP_NAME, e))
      raise # not the error we are looking for

   except:

      util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME)
      raise # not the error we are looking for

   finally: 

      # cleanup - remove local output file
      util.logMessage('Cleanup location \'%s\'' % output_dir)
      os.system("rm -rf \'%s\'" % output_dir) 
      os.system("rm -f \'%s\'" % output_gz_path) 



   return 0






if __name__ == "__main__":

   if len(sys.argv) < 4:
      util.logMessage("Error: param incorrect.")
      sys.exit(2)

   filename = sys.argv[2]
   outfilename = sys.argv[3]

   # Configure Spark
   spark = SparkSession \
      .builder \
      .appName(APP_NAME) \
      .getOrCreate()


   # Execute Main functionality
   ret = main(spark, filename, outfilename)
   if not ret == 0: 
      sys.exit(ret)

