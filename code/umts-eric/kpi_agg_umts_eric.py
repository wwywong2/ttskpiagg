#!/usr/bin/python

## Imports
import sys
import os
import glob # pathname
import shutil # move file
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
# argv[2] - input file (csv) e.g. "/mnt/nfs/test_results/eric/umts/set_*-*/set_001.txt"
# argv[3] - schema file (json) (empty if not needed) e.g. "umts_eric_schema.json"
# argv[4] - output parquet dir e.g. "/mnt/nfs/test/westest_umts.pqz"
# argv[5] - output csv e.g. "/mnt/nfs/test/westest_umts.csv"
# argv[6] - process mode: 'client' or 'cluster'

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



# argv[6] - process mode
proc_mode = ''
if len(sys.argv) > 6:
   proc_mode = sys.argv[6]
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
               if maindf is not None:
                  addPkAndSaveParquet(maindf, writemode, outputDir, numPartition)
               else:
                  util.logMessage("dataframe empty, no need to save to parquet")

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
   if maindf is not None:
      addPkAndSaveParquet(maindf, writemode, outputDir, numPartition)
   else:
      util.logMessage("dataframe empty, no need to save to parquet")


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



def aggKPI1(spark, pq, jsonFile, csv):

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

   sqlStr2 = "SELECT \
'unassigned' AS MGR_RUN_ID, \
MIN(Region) AS Region, \
MIN(Market) AS Market, \
MIN(SubNetwork_2) AS SubNetwork_2, \
HL_DATE AS PERIOD_START_TIME, \
%s \
CONCAT(SUBSTR(UtranCell,1,1),SUBSTR(UtranCell,-1,1),SUBSTR(UtranCell,-2,1)) AS HL_UMTSSort, \
HL_DATE AS HL_Date_Hour, \
HL_DATE AS HL_Date, \
UtranCell AS HL_Sector, \
case when substring(UtranCell,1,1) = 'U' THEN 'AWS' ELSE 'PCS' end AS HL_SectorLayer, \
concat('U',substring(UtranCell,2,8)) AS HL_Site, \
'unassigned' AS HL_Cluster, \
'unassigned' AS HL_Area, \
'unassigned' AS HL_Market \
FROM kpi \
[##where##] \
/*GROUP BY pk_date,pk_market,pk_hr,UtranCell,HL_UmtsSort,HL_Sector,HL_SectorLayer,HL_Site,HL_Cluster,HL_Area,HL_Market*/ \
GROUP BY HL_DATE,pk_date,pk_market,UtranCell \
/*ORDER BY pk_date,pk_market,pk_hr,HL_Site,HL_UMTSSort,UtranCell*/ " % (sqlStr)


   # from parquet dir get main info: datelist->marketlist->hrlist e.g. {"2016-11-21": {"NY": {"00": "path"}}}
   infoPq = getInfoFromPQ(pq)
   if len(infoPq.items()) <= 0: # safeguard
      util.logMessage("Error! No data found from parquet file: %s" % pq)
      return None

   util.logMessage("reading parquet: %s" % pq)
   df = spark.read.parquet(pq)
   util.logMessage("start aggregation process...")

   df.createOrReplaceTempView('kpi')

   # get latest date for now
   date,dateItem = sorted(infoPq.items(), reverse=True)[0] # only take the first time - lastest date

   # create csv by market
   for market,marketItem in dateItem.items():

      util.logMessage("creating csv for date: %s -- market: %s" % (date, market))

      #for hour,hourItem in marketItem.items(): # key2: hour; value: pathname
      #   util.logMessage("creating csv for hr: %s" % hour)

      numMaxHr = len(marketItem.items()) # get num of hours to run csv
      if numMaxHr > 24: # safeguard
         numMaxHr = 24

      for i in xrange(0,numMaxHr):

         whereStr = "WHERE pk_date = '%s' AND pk_market = '%s' AND pk_hr = '%02d' " % (date, market, i)

         # get uuid for MGR_RUN_ID - only temporary - future will have this column
         uuidstr = str(uuid.uuid4())

         # replace with real uuid
         sqlStrFinal = sqlStr2.replace("'unassigned' AS MGR_RUN_ID", "'%s' AS MGR_RUN_ID" % uuidstr)

         # replace where clause
         sqlStrFinal = sqlStrFinal.replace("[##where##]", whereStr)

         #print sqlStrFinal
         sqlDF = spark.sql(sqlStrFinal)

         # save df to csv if not empty
         if sqlDF.count() > 0:
            saveCsv(sqlDF, csv + "_%02d.csv" % i)


      if numMaxHr > 0: # if there is any hr data, create daily data

         # create overall csv (daily)
         whereStr = "WHERE pk_date = '%s' AND pk_market = '%s' " % (date, market)

         # get uuid for MGR_RUN_ID - only temporary - future will have this column
         uuidstr = str(uuid.uuid4())

         # replace with real uuid
         sqlStrFinal = sqlStr2.replace("'unassigned' AS MGR_RUN_ID", "'%s' AS MGR_RUN_ID" % uuidstr)

         # replace where clause
         sqlStrFinal = sqlStrFinal.replace("[##where##]", whereStr)

         # replace duration
         sqlStrFinal = sqlStrFinal.replace("(60) AS PERIOD_DURATION", "(1440) AS PERIOD_DURATION")

         # replace group by clause and hl_date col - WES_TEST: a bit static to wipe out hl_date group by
         sqlStrFinal = sqlStrFinal.replace("HL_DATE AS", "MIN(from_unixtime(unix_timestamp(HL_DATE, 'yyyy-MM-dd'), 'yyyy-MM-dd 00:00:00')) AS")
         sqlStrFinal = sqlStrFinal.replace("GROUP BY HL_DATE,", "GROUP BY ")

         #print sqlStrFinal
         sqlDF = spark.sql(sqlStrFinal)

         # save df to csv if not empty
         saveCsv(sqlDF, csv + ".csv")


   util.logMessage("finish aggregation process.")




def aggKPI2(spark, pq, jsonFile, csv):

   sqlStrFinal = ''
   sqlStr = ''

   try:
      with open(jsonFile) as json_data:
         sqlJson = json.load(json_data)
         for feature in sqlJson['features']:
            if feature['name'] == 'umts_eric': # only looking for umte eric feature
               #sqlStr = "SELECT " + sqlJson[0]['SELECT'] + " FROM kpi WHERE HL_DATE='2016-09-10 00:00:00' GROUP BY " + sqlJson[0]['GROUPBY']
               sqlStr = "SELECT " + feature['sql'][0]['SELECT'] + " FROM kpi [##where##] GROUP BY " + feature['sql'][0]['GROUPBY']
               break

      if sqlStr == '':
         util.logMessage("Error getting sql string from file: %s!" % jsonFile)
         return None

   except Exception as e:
      util.logMessage("Job: %s: Exception Error: %s!" % (APP_NAME, e))
      return None

   except:
      util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME)
      return None


   # from parquet dir get main info: datelist->marketlist->hrlist e.g. {"2016-11-21": {"NY": {"00": "path"}}}
   infoPq = getInfoFromPQ(pq)
   if len(infoPq.items()) <= 0: # safeguard
      util.logMessage("Error! No data found from parquet file: %s" % pq)
      return None

   util.logMessage("reading parquet: %s" % pq)
   df = spark.read.parquet(pq)
   util.logMessage("start aggregation process...")

   df.createOrReplaceTempView('kpi')

   # get latest date for now
   date,dateItem = sorted(infoPq.items(), reverse=True)[0] # only take the first time - lastest date

   # create csv by market
   for market,marketItem in dateItem.items():

      util.logMessage("creating csv for date: %s -- market: %s" % (date, market))

      #for hour,hourItem in marketItem.items(): # key2: hour; value: pathname         
      #   util.logMessage("creating csv for hr: %s" % hour)

      numMaxHr = len(marketItem.items()) # get num of hours to run csv
      if numMaxHr > 24: # safeguard
         numMaxHr = 24

      for i in xrange(0,numMaxHr):

         whereStr = "WHERE pk_date = '%s' AND pk_market = '%s' AND pk_hr = '%02d' " % (date, market, i)

         # get uuid for MGR_RUN_ID - only temporary - future will have this column
         uuidstr = str(uuid.uuid4())

         # replace with real uuid
         sqlStrFinal = sqlStr.replace("'unassigned' as MGR_RUN_ID", "'%s' as MGR_RUN_ID" % uuidstr)

         # replace where clause
         sqlStrFinal = sqlStrFinal.replace("[##where##]", whereStr)

         #print sqlStrFinal
         sqlDF = spark.sql(sqlStrFinal)

         # save df to csv if not empty
         if sqlDF.count() > 0:
            saveCsv(sqlDF, csv + "_%02d.csv" % i)


      if numMaxHr > 0: # if there is any hr data, create daily data

         # create overall csv (daily)
         whereStr = "WHERE pk_date = '%s' AND pk_market = '%s' " % (date, market)

         # get uuid for MGR_RUN_ID - only temporary - future will have this column
         uuidstr = str(uuid.uuid4())

         # replace with real uuid
         sqlStrFinal = sqlStr.replace("'unassigned' as MGR_RUN_ID", "'%s' as MGR_RUN_ID" % uuidstr)

         # replace where clause
         sqlStrFinal = sqlStrFinal.replace("[##where##]", whereStr)

         # replace duration
         sqlStrFinal = sqlStrFinal.replace("60 as PERIOD_DURATION", "1440 as PERIOD_DURATION")

         # replace group by clause and hl_date col - WES_TEST: a bit static to wipe out hl_date group by
         sqlStrFinal = sqlStrFinal.replace("hl_date as", "MIN(from_unixtime(unix_timestamp(hl_date, 'yyyy-MM-dd'), 'yyyy-MM-dd 00:00:00')) as")
         sqlStrFinal = sqlStrFinal.replace("GROUP BY hl_date,", "GROUP BY ")

         #print sqlStrFinal
         sqlDF = spark.sql(sqlStrFinal)

         # save df to csv if not empty
         saveCsv(sqlDF, csv + ".csv")


   util.logMessage("finish aggregation process.")





def getInfoFromPQ(parquetLocation):

   finalPqList = dict()
   pqList = glob.glob(parquetLocation+"/*_date=*")
   if len(pqList) <= 0:  # no date folder
      return None
   else:
      for date in pqList:

         dateStr = date.split("_date=")[1]
         finalPqList[dateStr] = dict()
      
         pqMarketList = glob.glob(date+"/*_market=*")
         if len(pqMarketList) <= 0:  # no date folder
            pass   
         else:
            for market in pqMarketList:

               marketStr = market.split("_market=")[1]
               finalPqList[dateStr][marketStr] = dict()

               pqHrList = glob.glob(market+"/*_hr=*")
               if len(pqHrList) <= 0:  # no date folder
                  pass
               else:
                  for hr in pqHrList:

                     hrStr = hr.split("_hr=")[1]
                     finalPqList[dateStr][marketStr][hrStr] = hr

   return finalPqList



  





def saveCsv(sqlDF, csv):

   #sqlDF.show(10, truncate=False)
   #util.logMessage("count: %d" % sqlDF.count())

   # output to csv file
   util.logMessage("save to csv: %s" % csv)

   csvTmp = csv+"."+time.strftime("%Y%m%d%H%M%S")+".tmp" # temp folder
   sqlDF.coalesce(1).write.csv(csvTmp,
                               header=True,
                               mode='overwrite',
                               sep=',',
                               dateFormat='yyyy-MM-dd',
                               timestampFormat='yyyy-MM-dd HH:mm:ss')
                               #timestampFormat='yyyy-MM-dd HH:mm:ss.SSS')

   # rename
   # check result file exist
   outputCsvList = glob.glob(csvTmp+"/*.csv")
   if len(outputCsvList) <= 0:  # no file
      util.logMessage("no file to output: %s" % csv)
      return None
   # supposed only have 1 because of coalesce(1), but in case of more than one, it will just keep overwriting
   for curr_file in sorted(outputCsvList):
      os.system("rm -rf "+csv) # remove prev output
      shutil.move(curr_file, csv)
   os.system("rm -rf "+csvTmp) # remove temp output folder





def main(spark,inCSV,outPQ,outCSV):


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


      if inCSV is not "":

         # get schema
         schema = None # init
         #schemaFile = "umts_eric_schema.json"
         if schemaFile is not "":
            #schema = jsonToSchema(SparkFiles.get(schemaFile)) # might need this one when running cluster mode
            schema = jsonToSchema(curr_py_dir+'/'+schemaFile)

         if schema is not None:
            util.logMessage("acquired schema from file: %s" % schemaFile)
         else:
            util.logMessage("assumed no schema")


         # read csv file(s) into dataframe then save into parquet
         #csvToParquet1(spark, inCSV, schema, outPQ) # old way - read 1 save 1
         csvToParquet2(spark, inCSV, schema, outPQ, 20, numPartition=4) # new way - read 20 save 1

         # sample code
         #sampleCode(spark)

      # end of if inCSV is not "":



      # aggregation by hour and save to csv
      if outCSV is not "":
         #aggKPI1(spark, outPQ, curr_py_dir+'/'+schemaFile, outCSV) # grab sql info from schema file itself
         aggKPI2(spark, outPQ, curr_py_dir+'/'+sqlFile, outCSV) # grab sql info from sql file



      #time.sleep(120)


   except Exception as e:

      #util.logMessage('Cleanup location \'%s\'' % outPQ)
      #os.system("rm -rf \'%s\'" % outPQ) 
      util.logMessage("Job: %s: Exception Error: %s!" % (APP_NAME, e))
      raise

   except:

      #util.logMessage('Cleanup location \'%s\'' % outPQ)
      #os.system("rm -rf \'%s\'" % outPQ) 
      util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME)
      raise # not the error we are looking for




   # stop spark session (and context)
   spark.stop()

   util.logMessage("finish process successfully.")
   return 0







if __name__ == "__main__":

   if len(sys.argv) < 6:
      util.logMessage("Error: param incorrect.")
      sys.exit(2)

   inCSV = sys.argv[2]
   outPQ = output_dir
   outCSV = sys.argv[5]

   # Configure Spark
   spark = SparkSession \
      .builder \
      .appName(APP_NAME) \
      .getOrCreate()


   # Execute Main functionality
   ret = main(spark, inCSV, outPQ, outCSV)
   if not ret == 0: 
      sys.exit(ret)

