#!/usr/bin/python

## Imports
import sys
import os
import glob # pathname
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
# argv[3] - schema file (json) (empty if not needed)
# argv[4] - output parquet dir
# argv[5] - process mode: 'client' or 'cluster'

APP_NAME = "kpiAggrApp"
# argv[1] - take app name from param
if len(sys.argv) > 1:
   APP_NAME = sys.argv[1]

# argv[3] - schema filename
schemaFile = ""
if len(sys.argv) > 3:
   schemaFile = sys.argv[3]
sqlFile = schemaFile.replace('_schema', '_sql')



# argv[4] - output dir
output_dir = ""
if len(sys.argv) > 4:
   output_dir = sys.argv[4]
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



# argv[5] - process mode
proc_mode = ''
if len(sys.argv) > 5:
   proc_mode = sys.argv[5]
proc_mode = proc_mode.lower()
if not proc_mode == 'cluster':
   proc_mode = 'client'





##OTHER FUNCTIONS/CLASSES


def sampleCode(spark):

   # read parquet
   pqDF = spark.read.parquet('/mnt/nfs/test/eric_new3.pqz')
   pqDF.show(30, truncate=False)

   # create view for sql
   pqDF.createOrReplaceTempView('eric')

   # sql
   sqlDF = spark.sql("SELECT distinct HL_MARKET FROM eric")
   sqlDF.show()

   # write to parquet, consolidate to 1 file (takes time)
   sqlDF.coalesce(1).write.parquet('/mnt/nfs/test/eric_new4.pqz',
          compression='gzip',
          mode='overwrite',
          partitionBy='newtime')

   # drop a view
   spark.catalog.dropTempView("eric")



def csvToDF(spark, csvFile, schema=None):

   try:
      if schema is None:
         df = spark.read.csv(csvFile,
                             ignoreLeadingWhiteSpace=True,
                             ignoreTrailingWhiteSpace=True,
                             header=True,
                             timestampFormat='yyyy-MM-dd HH:mm')
      else:
         df = spark.read.csv(csvFile,
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


# read each csv into df then export to parquet
def csvToParquet1(spark, inputCsv, schema, outputDir, numPartition=None):

   # read csv file(s) into dataframe
   filecount = 0 # init

   # check file exist
   inputCsvList = glob.glob(inputCsv)
   #if len(glob.glob(inputCsv)) <= 0:  # no file
   if len(inputCsvList) <= 0:  # no file
      util.logMessage("no file to process: %s" % inputCsv)
      util.logMessage("Process terminated.")
      return 0

   for curr_file in sorted(inputCsvList):

      filecount += 1
      util.logMessage("reading file: %s" % curr_file)

      df = csvToDF(spark, curr_file, schema)
      if df is None:
         util.logMessage("issue(s) reading file: %s" % curr_file)
      else:

         util.logMessage("finish reading file: %s" % curr_file)

         # write parquet
         if filecount == 1:
            writemode = 'overwrite'
         else:
            writemode = 'append'
         addPkAndSaveParquet(df, writemode, outputDir, numPartition)

   return 0




# read group of csvs and union into df then export to parquet
def csvToParquet2(spark, inputCsv, schema, outputDir, loadFactor=10, numPartition=None):

   # read csv file(s) into dataframe
   filecount = 0 # init
   firsttime = True

   # check file exist
   inputCsvList = glob.glob(inputCsv)
   #if len(glob.glob(inputCsv)) <= 0:  # no file
   if len(inputCsvList) <= 0:  # no file
      util.logMessage("no file to process: %s" % inputCsv)
      util.logMessage("Process terminated.")
      return 0

   for curr_file in sorted(inputCsvList):

      util.logMessage("reading file: %s" % curr_file)

      df = csvToDF(spark, curr_file, schema)
      if df is None:
         util.logMessage("issue(s) reading file: %s" % curr_file)
      else:
         util.logMessage("finish reading file: %s" % curr_file)

      if filecount % loadFactor == 0:
         if filecount is not 0: # not the first time, write/append to file

            # write parquet
            if firsttime:
               writemode = 'overwrite'
               firsttime = False
            else:
               writemode = 'append'
            addPkAndSaveParquet(maindf, writemode, outputDir, numPartition)

         maindf = df
      else:
         maindf = maindf.union(df)

      filecount += 1
      # end of for curr_file in sorted(inputCsvList):


   if firsttime:
      writemode = 'overwrite'
      firsttime = False
   else:
      writemode = 'append'
   addPkAndSaveParquet(maindf, writemode, outputDir, numPartition)


   return 0



def addPkAndSaveParquet(df, writemode, outputDir, numPartition=None):

   util.logMessage("adding partition columns...")

   '''
   # add key col from HL_Area
   df = df.withColumn("HL_Area", lit('unassigned'))
   # add key col from HL_Cluster
   df = df.withColumn("HL_Cluster", lit('unassigned'))
   # add key col from HL_SectorLayer
   df = df.withColumn("HL_SectorLayer", lit(None).cast(StringType()))
   '''

   # add key col from HL_MARKET - need to add HL_MARKET because that column will be gone if we go into sub dir
   df = df.withColumn("pk_market", df['HL_MARKET'])
   # add key col from HL_DATE
   df = df.withColumn("pk_date", date_format(df['HL_DATE'], 'yyyy-MM-dd'))
   # add key col from PERIOD_START_TIME
   df = df.withColumn("pk_hr", date_format(df['PERIOD_START_TIME'], 'HH'))


   # show dtypes
   #util.logMessage("dtypes: %s" % df.dtypes)
   # show schema
   #df.printSchema()
   #df.show(1,truncate=False)

   util.logMessage("start writing parquet file (%s): %s" % (writemode, outputDir))
   if numPartition is None:
      df.write.parquet(outputDir,
                       compression='gzip',
                       mode=writemode,
                       partitionBy=('pk_date','pk_market','pk_hr'))
   else:
      # coalesce - num of partition - should match number of executor we run
      df.coalesce(numPartition).write.parquet(outputDir,
                       compression='gzip',
                       mode=writemode,
                       partitionBy=('pk_date','pk_market','pk_hr'))
   util.logMessage("finish writing parquet file (%s): %s" % (writemode, outputDir))     


def aggKPI1(spark, pq, jsonFile):

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

   # compose kpi aggregation string
   # case: MIN, SUM, EXCLUDE, anything else is as-is e.g. formulas, string, constants
   sqlStr = ''
   for item in schemaJson:
      if 'agg' not in item: # no agg property, don't include that column
         sqlStr += ""
      elif item['agg'] == 'MIN':
         sqlStr += "MIN(%s) AS %s, " % (item['name'], item['name'])
      elif item['agg'] == 'MAX':
         sqlStr += "MAX(%s) AS %s, " % (item['name'], item['name'])
      elif item['agg'] == 'SUM':
         sqlStr += "SUM(%s) AS %s, " % (item['name'], item['name'])
      elif item['agg'] == 'EXCLUDE':
         sqlStr += ""
      else: # all else
         sqlStr += "(%s) AS %s, " % (item['agg'], item['name'])


   util.logMessage("reading parquet: %s" % pq)
   df = spark.read.parquet(pq)
   util.logMessage("start aggregation process...")

   df.createOrReplaceTempView('kpi')

   # get uuid for MGR_RUN_ID
   uuidstr = str(uuid.uuid4())

   sqlStrFinal = "SELECT \
'%s' AS MGR_RUN_ID, \
MIN(Region) AS Region, \
MIN(Market) AS Market, \
MIN(SubNetwork_2) AS SubNetwork_2, \
HL_DATE AS PERIOD_START_TIME, \
%s \
HL_DATE AS HL_Date_Hour, \
HL_DATE AS HL_Date, \
EUtranCellFDD AS HL_Sector, \
'' AS HL_SectorLayer, \
MeContext AS HL_Site, \
'unassigned' AS HL_Cluster, \
'unassigned' AS HL_Area, \
'unassigned' AS HL_Market \
FROM kpi \
/*GROUP BY pk_date,pk_market,pk_hr,MeContext,EUtranCellFDD,HL_SectorLayer,HL_Cluster,HL_Area*/ \
GROUP BY pk_date,pk_market,pk_hr,MeContext,EUtranCellFDD,HL_DATE \
ORDER BY pk_date,pk_market,pk_hr,MeContext,EUtranCellFDD" % (uuidstr, sqlStr)

   #print sqlStrFinal
   sqlDF = spark.sql(sqlStrFinal)

   # output to csv file
   #sqlDF.show(10, truncate=False)
   #util.logMessage("count: %d" % sqlDF.count())
   util.logMessage("save to csv....")

   #csvDir, csvFile = os.path.split(pq)
   #csvFile += '.csv'
   csvLocation = pq + '.csv'
   
   sqlDF.coalesce(1).write.csv(csvLocation, 
                               header=True, 
                               mode='overwrite', 
                               sep=',', 
                               dateFormat='yyyy-MM-dd', 
                               timestampFormat='yyyy-MM-dd HH:mm:ss')
                               #timestampFormat='yyyy-MM-dd HH:mm:ss.SSS')


   util.logMessage("finish aggregation process.")





def aggKPI2(spark, pq, jsonFile):

   try:
      with open(jsonFile) as json_data:
         sqlJson = json.load(json_data)
         #sqlStrFinal = "SELECT " + sqlJson[0]['SELECT'] + " FROM kpi WHERE HL_DATE='2016-09-10 00:00:00' GROUP BY " + sqlJson[0]['GROUPBY']
         sqlStrFinal = "SELECT " + sqlJson[0]['SELECT'] + " FROM kpi GROUP BY " + sqlJson[0]['GROUPBY']

   except Exception as e:
      util.logMessage("Job: %s: Exception Error: %s!" % (APP_NAME, e))
      return None

   except:
      util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME)
      return None

   util.logMessage("reading parquet: %s" % pq)
   df = spark.read.parquet(pq)
   util.logMessage("start aggregation process...")

   df.createOrReplaceTempView('kpi')

   # get uuid for MGR_RUN_ID - only temporary - future will have this column
   uuidstr = str(uuid.uuid4())
   # replace with real uuid
   sqlStrFinal = sqlStrFinal.replace("'unassigned' as MGR_RUN_ID", "'%s' as MGR_RUN_ID" % uuidstr)

   #print sqlStrFinal
   sqlDF = spark.sql(sqlStrFinal)

   # output to csv file
   #sqlDF.show(10, truncate=False)
   #util.logMessage("count: %d" % sqlDF.count())
   util.logMessage("save to csv....")

   #csvDir, csvFile = os.path.split(pq)
   #csvFile += '.csv'
   csvLocation = pq + '.csv'
   
   sqlDF.coalesce(1).write.csv(csvLocation, 
                               header=True, 
                               mode='overwrite', 
                               sep=',', 
                               dateFormat='yyyy-MM-dd', 
                               timestampFormat='yyyy-MM-dd HH:mm:ss')
                               #timestampFormat='yyyy-MM-dd HH:mm:ss.SSS')


   util.logMessage("finish aggregation process.")



def main(spark,filename,outfilename):


   try:

      sc = spark.sparkContext

      if proc_mode == 'client':

         # add file        
         if schemaFile is not "":
            util.logMessage("addFile: %s" % curr_py_dir+'/'+schemaFile)
            sc.addFile(curr_py_dir+'/'+schemaFile)
         '''
         # need to enable when run cluster mode
         if sqlFile is not "":
            util.logMessage("addFile: %s" % curr_py_dir+'/'+sqlFile)
            sc.addFile(curr_py_dir+'/'+sqlFile)
         '''

         # add py reference
         util.logMessage("addPyFile: %s" % curr_py_dir+'/util.py')
         sc.addPyFile(curr_py_dir+'/util.py')




      util.logMessage("process start...")


      # get schema
      schema = None # init
      #schemaFile = "lte_eric_schema.json"
      if schemaFile is not "":
         #schema = jsonToSchema(SparkFiles.get(schemaFile)) # might need this one when running cluster mode
         schema = jsonToSchema(curr_py_dir+'/'+schemaFile)

      if schema is not None:
         util.logMessage("acquired schema from file: %s" % schemaFile)
      else:
         util.logMessage("assumed no schema")


      # read csv file(s) into dataframe then save into parquet
      #csvToParquet1(spark, filename, schema, output_dir) # old way - read 1 save 1
      csvToParquet2(spark, filename, schema, output_dir, 10, numPartition=4) # new way - read 20 save 1

      # sample code
      #sampleCode(spark)

      # aggregation by hour and save to csv
      #aggKPI1(spark, output_dir, curr_py_dir+'/'+schemaFile) # grab sql info from schema file itself
      aggKPI2(spark, output_dir, curr_py_dir+'/'+sqlFile) # grab sql info from sql file 



      #time.sleep(120)


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




   # stop spark session (and context)
   spark.stop()

   util.logMessage("finish process successfully.")
   return 0



if __name__ == "__main__":

   if len(sys.argv) < 4:
      util.logMessage("Error: param incorrect.")
      sys.exit(2)

   filename = sys.argv[2]
   outfilename = sys.argv[4]

   # Configure Spark
   spark = SparkSession \
      .builder \
      .appName(APP_NAME) \
      .getOrCreate()


   # Execute Main functionality
   ret = main(spark, filename, outfilename)
   if not ret == 0: 
      sys.exit(ret)
