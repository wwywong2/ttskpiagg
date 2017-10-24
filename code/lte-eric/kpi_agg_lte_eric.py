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
# argv[2] - input file (csv) e.g. "/mnt/nfs/test_results/eric/lte/set_*-*/set_001.txt"; if empty, skip to next stage
# argv[3] - schema file (json) (empty if not needed) e.g. "lte_eric_schema.json"
# argv[4] - input cell lookup parquet  e.g. "/mnt/nfs/test/westest_CellLookup.pqz"
# argv[5] - output parquet dir e.g. "/mnt/nfs/test/westest_lte.pqz"
# argv[6] - output csv e.g. "/mnt/nfs/test/westest_lte"; if empty, not creating export
# argv[7] - option json string (optional); default '{"overwrite":false, "partiaionNum":2, "loadFactor":3}'
#           "partitionNum":null --> None in python (no coalesce)
#           "overwrite":false/true --> False/True in python
# argv[8] - process mode: 'client' or 'cluster'
def printUsage():
   print '\nUsage:'
   print '%s procName inFile inSchemaFile inLookupPQ outPQ outCSV optionJSON\n' % (curr_py_filename)
   print 'e.g.'
   print '/opt/spark/bin/spark-submit --master mesos://zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos --driver-memory 512M --executor-memory 2G --total-executor-cores 8 %s "testProc" "/mnt/nfs/test/eric/lte/set_*_100/set_00*.txt" "lte_eric_schema.json" "/mnt/nfs/test/cellLookup.pqz" "/mnt/nfs/test/out.pqz" "/mnt/nfs/test/testLte" \'{"overwrite":false, "partitionNum":2, "loadFactor":3}\' \n' % (curr_py_filename)

   print 'Detail:'
   print '   procName     - process name (mandatory)'
   print '   inFile       - input file (csv) e.g. "/mnt/nfs/test/set_*-*/set_00*.txt";'
   print '                  if empty, only do aggregation on existing parquet & export to csv'
   print '   inSchemaFile - schema file (json) (empty if not needed) e.g. "lte_eric_schema.json"'
   print '   inLookupPQ   - input cell lookup parquet (mandatory) e.g. "/mnt/nfs/test/cellLookup.pqz"'
   print '   outPQ        - output parquet dir (mandatory) e.g. "/mnt/nfs/test/outLte.pqz"'
   print '   outCSV       - output csv in tgz package (w/o extension);  e.g. "/mnt/nfs/test/lteExport";'
   print '                  if empty, not creating export, just process kpi into parquet'
   print '   optionJSON   - option json string (optional)'
   print '                  default \'{"overwrite":false, "partiaionNum":2, "loadFactor":3}\''
   print '                  "overwrite":false/true --> False/True in python; true - overwrite outPQ'
   print '                  "partitionNum":null --> None in python (no coalesce); should match with executor#'
   print '                  "loadFactor":3 --> # of seq file to join before writing to outPQ'
   #print '   procMode     - process mode: "client" or "cluster"; default: client'
   print 





if len(sys.argv) < 7:
   util.logMessage("Error: param incorrect.")
   printUsage()
   sys.exit(2)



APP_NAME = "kpiAggrApp"
# argv[1] - take app name from param
if len(sys.argv) > 1:
   APP_NAME = sys.argv[1]

# argv[2] - input file
inCSV = sys.argv[2]

# argv[3] - schema filename
schemaFile = ""
if len(sys.argv) > 3:
   schemaFile = sys.argv[3]
sqlFile = schemaFile.replace('_schema', '_sql')

# argv[4] - input cell lookup parquet
inCellLookupPQ = sys.argv[4]
# quick check
inCellLookupPQ = inCellLookupPQ.rstrip('/')
if inCellLookupPQ == '':
   util.logMessage("lookup parquet location cannot be empty.")
   printUsage()
   sys.exit(2)
elif not os.path.isdir(inCellLookupPQ):  # error out if not exist
   util.logMessage("lookup parquet \"%s\" does not exist!" % inCellLookupPQ)
   printUsage()
   sys.exit(2)

# argv[5] - output parquet
output_dir = ""
if len(sys.argv) > 5:
   output_dir = sys.argv[5]
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
outPQ = output_dir
# quick check
if not os.path.isdir(outPQ):  # error out if not exist
   util.logMessage("output parquet location \"%s\" does not exist!" % outPQ)
   printUsage()
   sys.exit(2)

# argv[6] - output csv
outCSV = sys.argv[6]
outCSV = outCSV.rstrip('/')

# argv[7] - option json
optionJSON = ""
if len(sys.argv) > 7:
   optionJSON = sys.argv[7]
if optionJSON == "":
   optionJSON = '{"overwrite":false, "partitionNum":2, "loadFactor":3}'
try:
   optionJSON = json.loads(optionJSON)
except Exception as e: # error parsing json
   optionJSON = '{"overwrite":false, "partitionNum":2, "loadFactor":3}'
   optionJSON = json.loads(optionJSON) 
# default val if not exist
if 'overwrite' not in optionJSON:
   optionJSON[u'overwrite'] = False
if 'partitionNum' not in optionJSON:
   optionJSON[u'partitionNum'] = 2
if 'loadFactor' not in optionJSON:
   optionJSON[u'loadFactor'] = 3
util.logMessage("Process start with option:\n%s" % optionJSON)

# argv[8] - process mode
proc_mode = ''
if len(sys.argv) > 8:
   proc_mode = sys.argv[8]
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
def csvToParquet1(spark, inputCsv, schema, lookupPQ, outputDir, numPartition=None, overwrite=False):

   # read csv file(s) into dataframe
   filecount = 0 # init

   # check file exist
   inputCsvList = glob.glob(inputCsv)
   #if len(glob.glob(inputCsv)) <= 0:  # no file
   if len(inputCsvList) <= 0:  # no file
      util.logMessage("no file to process: %s" % inputCsv)
      #util.logMessage("Process terminated.")
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
         if filecount == 1 and overwrite:
            writemode = 'overwrite'
         else:
            writemode = 'append'
         addPkAndSaveParquet(df, lookupPQ, writemode, outputDir, numPartition)

   return 0




# read group of csvs and union into df then export to parquet
def csvToParquet2(spark, inputCsv, schema, lookupPQ, outputDir, loadFactor=10, numPartition=None, overwrite=False):

   # read csv file(s) into dataframe
   filecount = 0 # init
   firsttime = True

   # check file exist
   inputCsvList = glob.glob(inputCsv)
   #if len(glob.glob(inputCsv)) <= 0:  # no file
   if len(inputCsvList) <= 0:  # no file
      util.logMessage("no file to process: %s" % inputCsv)
      #util.logMessage("Process terminated.")
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
               if firsttime and overwrite:
                  writemode = 'overwrite'
                  firsttime = False
               else:
                  writemode = 'append'
               if maindf is not None:
                  addPkAndSaveParquet(maindf, lookupPQ, writemode, outputDir, numPartition)
               else:
                  util.logMessage("dataframe empty, no need to save to parquet")

            maindf = df
         else:
            maindf = maindf.union(df)

      filecount += 1
      # end of for curr_file in sorted(inputCsvList):


   if firsttime and overwrite:
      writemode = 'overwrite'
      firsttime = False
   else:
      writemode = 'append'
   if maindf is not None:
      addPkAndSaveParquet(maindf, lookupPQ, writemode, outputDir, numPartition)
   else:
      util.logMessage("dataframe empty, no need to save to parquet")


   return 0



def addPkAndSaveParquet(origDF, lookupPQ, writemode, outputDir, numPartition=None):

   util.logMessage("adding partition columns...")

   '''
   # add key col from HL_Area
   origDF = origDF.withColumn("HL_Area", lit('unassigned'))
   # add key col from HL_Cluster
   origDF = origDF.withColumn("HL_Cluster", lit('unassigned'))
   # add key col from HL_SectorLayer
   origDF = origDF.withColumn("HL_SectorLayer", lit(None).cast(StringType()))
   '''

   # remove HL_Market column; it will be recovered later
   origDF = origDF.drop("HL_Market")
   origDF.createOrReplaceTempView('kpi')

   # recover from lookup parquet
   # read parquet
   util.logMessage("reading lookup parquet: %s" % lookupPQ)
   dfLookup = spark.read.parquet(lookupPQ)
   dfLookup.createOrReplaceTempView('lookup')
   util.logMessage("start market-cluster-area recovery process...")

   # example join sql
   #sqlDF = spark.sql("SELECT l.TECH,l.VENDOR,l.MARKET,l.CLUSTER,l.AREA,k.EUtranCellFDD from kpi k left join lookup l on k.EUtranCellFDD = l.CELL")
   # create join dataframe
   df = spark.sql("SELECT k.*, IFNULL(l.MARKET,'unassigned') as HL_Market, IFNULL(l.CLUSTER,'unassigned') AS HL_Cluster, IFNULL(l.AREA,'unassigned') AS HL_Area from kpi k left join lookup l on k.EUtranCellFDD = l.CELL AND l.TECH = 'LTE'")

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
HL_DATE AS HL_Date_Hour, \
HL_DATE AS HL_Date, \
EUtranCellFDD AS HL_Sector, \
'' AS HL_SectorLayer, \
MeContext AS HL_Site, \
'unassigned' AS HL_Cluster, \
'unassigned' AS HL_Area, \
'unassigned' AS HL_Market \
FROM kpi \
[##where##] \
/*GROUP BY pk_date,pk_market,pk_hr,MeContext,EUtranCellFDD,HL_SectorLayer,HL_Cluster,HL_Area*/ \
GROUP BY HL_DATE,pk_date,pk_market,pk_hr,MeContext,EUtranCellFDD \
/*ORDER BY pk_date,pk_market,pk_hr,MeContext,EUtranCellFDD*/ " % (sqlStr)


   # from parquet dir get main info: datelist->marketlist->hrlist e.g. {"2016-11-21": {"NY": {"00": "path"}}}
   infoPq = getInfoFromPQ(pq)
   if len(infoPq.items()) <= 0: # safeguard
      util.logMessage("Error! No data found from parquet file: %s" % pq)
      return None

   # read parquet
   util.logMessage("reading parquet: %s" % pq)
   df = spark.read.parquet(pq)
   df.createOrReplaceTempView('kpi')
   util.logMessage("start aggregation process...")


   # get latest date for now
   date,dateItem = sorted(infoPq.items(), reverse=True)[0] # only take the first time - lastest date

   # create csv by market
   for market,marketItem in dateItem.items():

      #for hour,hourItem in marketItem.items(): # key2: hour; value: pathname
      #   util.logMessage("creating csv for hr: %s" % hour)

      util.logMessage("creating csv for date: %s -- market: %s" % (date, market))

      numMaxHr = len(marketItem.items()) # get num of hours to run csv
      if numMaxHr > 24: # safeguard
         numMaxHr = 24

      # create hourly csv
      for i in xrange(0,numMaxHr):

         # get uuid for MGR_RUN_ID
         uuidstr = str(uuid.uuid4())
         # replace with real uuid
         sqlStrFinal = sqlStr2.replace("'unassigned' AS MGR_RUN_ID", "'%s' AS MGR_RUN_ID" % uuidstr)

         # replace where clause
         whereStr = "WHERE pk_date = '%s' AND pk_market = '%s' AND pk_hr = '%02d' " % (date, market, i)
         sqlStrFinal = sqlStrFinal.replace("[##where##]", whereStr)

         #print sqlStrFinal
         sqlDF = spark.sql(sqlStrFinal)

         # save df to csv if not empty
         if sqlDF.count() > 0:
            saveCsv(sqlDF, csv + "_%s_%02d.csv" % (market,i))


      # create daily csv
      if numMaxHr > 0: # if there is any hr data, create daily data

         # get uuid for MGR_RUN_ID
         uuidstr = str(uuid.uuid4())
         # replace with real uuid
         sqlStrFinal = sqlStr2.replace("'unassigned' AS MGR_RUN_ID", "'%s' AS MGR_RUN_ID" % uuidstr)

         # replace where clause
         whereStr = "WHERE pk_date = '%s' AND pk_market = '%s' " % (date, market)
         sqlStrFinal = sqlStrFinal.replace("[##where##]", whereStr)

         # replace group by clause and hl_date col - to remove hl_date(hourly) group by
         sqlStrFinal = sqlStrFinal.replace("HL_DATE AS", "MIN(from_unixtime(unix_timestamp(HL_DATE, 'yyyy-MM-dd'), 'yyyy-MM-dd 00:00:00')) AS")
         sqlStrFinal = sqlStrFinal.replace("GROUP BY HL_DATE,", "GROUP BY ")

         #print sqlStrFinal
         sqlDF = spark.sql(sqlStrFinal)

         # save df to csv if not empty
         saveCsv(sqlDF, csv + "_%s.csv" % market)


   util.logMessage("finish aggregation process.")




def aggKPI2(spark, pq, jsonFile, csv):

   sqlStrFinal = ''
   sqlStr = ''

   try:
      with open(jsonFile) as json_data:
         sqlJson = json.load(json_data)
         for feature in sqlJson['features']:
            if feature['name'] == 'lte_eric': # only looking for lte eric feature
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

   # read parquet
   util.logMessage("reading parquet: %s" % pq)
   df = spark.read.parquet(pq)
   df.createOrReplaceTempView('kpi')
   util.logMessage("start aggregation process...")


   # get latest date for now
   date,dateItem = sorted(infoPq.items(), reverse=True)[0] # only take the first time - lastest date

   # create csv by market
   for market,marketItem in dateItem.items():

      #for hour,hourItem in marketItem.items():
      #   util.logMessage("creating csv for hr: %s" % hour)

      util.logMessage("creating csv for date: %s -- market: %s" % (date, market))

      numMaxHr = len(marketItem.items()) # get num of hours to run csv
      if numMaxHr > 24: # safeguard
         numMaxHr = 24

      # create hourly csv
      for i in xrange(0,numMaxHr):

         # get uuid for MGR_RUN_ID
         uuidstr = str(uuid.uuid4())
         # replace with real uuid
         sqlStrFinal = sqlStr.replace("'unassigned' as MGR_RUN_ID", "'%s' as MGR_RUN_ID" % uuidstr)

         # replace where clause
         whereStr = "WHERE pk_date = '%s' AND pk_market = '%s' AND pk_hr = '%02d' " % (date, market, i)
         sqlStrFinal = sqlStrFinal.replace("[##where##]", whereStr)

         #print sqlStrFinal
         sqlDF = spark.sql(sqlStrFinal)

         # save df to csv if not empty
         if sqlDF.count() > 0:
            saveCsv(sqlDF, csv + "_%s_%02d.csv" % (market,i))


      # create daily csv
      if numMaxHr > 0: # if there is any hr data, create daily data

         # get uuid for MGR_RUN_ID
         uuidstr = str(uuid.uuid4())
         # replace with real uuid
         sqlStrFinal = sqlStr.replace("'unassigned' as MGR_RUN_ID", "'%s' as MGR_RUN_ID" % uuidstr)

         # replace where clause
         whereStr = "WHERE pk_date = '%s' AND pk_market = '%s' " % (date, market)
         sqlStrFinal = sqlStrFinal.replace("[##where##]", whereStr)

         # replace group by clause and hl_date col - to remove hl_date(hourly) group by
         sqlStrFinal = sqlStrFinal.replace("hl_date as", "MIN(from_unixtime(unix_timestamp(hl_date, 'yyyy-MM-dd'), 'yyyy-MM-dd 00:00:00')) as")
         sqlStrFinal = sqlStrFinal.replace("GROUP BY hl_date,", "GROUP BY ")

         #print sqlStrFinal
         sqlDF = spark.sql(sqlStrFinal)

         # save df to csv if not empty
         saveCsv(sqlDF, csv + "_%s.csv" % market)


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
         if len(pqMarketList) <= 0:  # no market folder
            pass   
         else:
            for market in pqMarketList:

               marketStr = market.split("_market=")[1]
               finalPqList[dateStr][marketStr] = dict()

               pqHrList = glob.glob(market+"/*_hr=*")
               if len(pqHrList) <= 0:  # no hr folder
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




def createCellLookup(spark, inCSV, outPQ):

   df = spark.read.csv(inCSV,
                       ignoreLeadingWhiteSpace=True,
                       ignoreTrailingWhiteSpace=True,
                       header=True,
                       timestampFormat='yyyy-MM-dd HH:mm')

   df.write.parquet(outPQ,
                    compression='gzip',
                    mode='overwrite',
                    partitionBy=('TECH','VENDOR','MARKET'))





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


      # test creating cell lookup parquet
      #createCellLookup(spark, inCSV, outPQ) # '/mnt/nfs/test/westest_CellLookup.pqz'
      #return 0

      if inCSV is not "":

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
         #csvToParquet1(spark, inCSV, schema, inCellLookupPQ, outPQ, numPartition=optionJSON['partitionNum'], overwrite=optionJSON['overwrite']) # old way - read 1 save 1
         csvToParquet2(spark, inCSV, schema, inCellLookupPQ, outPQ, optionJSON['loadFactor'], numPartition=optionJSON['partitionNum'], overwrite=optionJSON['overwrite']) # new way - read 20 save 1
         # note: 2 partition - 2G exec mem - 4 cores (2 exec) - 3 union - ok
         # note: 2 partition - 2G exec mem - 4 cores (2 exec) - 5 union - ok?
         # note: 4 partition - 2G exec mem - 8 cores (4 exec) - 10 union - ok
         

         # sample code
         #sampleCode(spark)

      # end of if inCSV is not "":



      # aggregation by hour and save to csv
      if outCSV is not "":

         outCSV = outCSV.rstrip('/')
         outCSVdir1, outCSVdir2 = os.path.split(outCSV)
         outCSVTmp = outCSVdir1 + '/' + outCSVdir2 + '_' + time.strftime("%Y%m%d%H%M%S")
         # create tmp folder
         try:
            os.system("rm -rf "+outCSVTmp) # remove prev output
            os.mkdir(outCSVTmp)
         except Exception as e:
            util.logMessage("failed to create folder '%s'!\n%s" % (outCSVTmp,e))
            os.system("rm -rf "+outCSVTmp) # remove temp output folder
            return 0
         except:
            util.logMessage("failed to create folder '%s'!" % outCSVTmp)
            os.system("rm -rf "+outCSVTmp) # remove temp output folder
            return 0

         # run aggregation
         try:
            #aggKPI1(spark, outPQ, curr_py_dir+'/'+schemaFile, outCSVTmp+'/'+outCSVdir2) # grab sql info from schema file itself
            aggKPI2(spark, outPQ, curr_py_dir+'/'+sqlFile, outCSVTmp+'/'+outCSVdir2) # grab sql info from sql file
         except Exception as e:
            util.logMessage("failed to aggregate to '%s'!\n%s" % (outCSVTmp,e))
            os.system("rm -rf "+outCSVTmp) # remove temp output folder
            return 0
         except:
            util.logMessage("failed to aggregate to '%s'!" % outCSVTmp)
            os.system("rm -rf "+outCSVTmp) # remove temp output folder
            return 0

         
         # zip to file
         outCSVTgz = outCSVdir2+'.tgz'
         try:
            util.logMessage('zipping files: cd %s && tar -cvzf %s *.csv' % (outCSVTmp, outCSVTgz))
            os.system("cd %s && tar -cvzf %s *.csv" % (outCSVTmp, outCSVTgz))
            os.system("rm -rf "+outCSVdir1+'/'+outCSVTgz) # remove old output file
            shutil.move(outCSVTmp+'/'+outCSVTgz, outCSVdir1+'/'+outCSVTgz)
            os.system("rm -rf "+outCSVTmp) # remove temp output folder
            util.logMessage('zipping files successful: %s' % outCSVdir1+'/'+outCSVTgz)
         except Exception as e:
            util.logMessage("failed to zip file '%s'!\n%s" % (outCSVTgz,e))
            os.system("rm -rf "+outCSVTmp) # remove temp output folder
            return 0
         except:
            util.logMessage("failed to zip file '%s'!" % outCSVTgz)
            os.system("rm -rf "+outCSVTmp) # remove temp output folder
            return 0




   except Exception as e:
      util.logMessage("Job: %s: Exception Error: %s!" % (APP_NAME, e))
      raise

   except:
      util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME)
      raise # not the error we are looking for




   # stop spark session (and context)
   spark.stop()

   util.logMessage("finish process successfully.")
   return 0







if __name__ == "__main__":


   # Configure Spark
   spark = SparkSession \
      .builder \
      .appName(APP_NAME) \
      .getOrCreate()


   # Execute Main functionality
   ret = main(spark, inCSV, outPQ, outCSV)
   if not ret == 0: 
      sys.exit(ret)

