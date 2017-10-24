from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
import pyspark.sql.functions as pysparksqlfunc

import os, sys, glob, time
import json, util, uuid, linecache
import subprocess, datetime, shutil

"""
A simple example demonstrating Spark SQL data sources.
Run with:
  ./bin/spark-submit testspqrksql.py resultrootpath
"""

def getException():
    expobj = {}
    
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    
    expobj['filename'] = filename
    expobj['linenumber'] = lineno
    expobj['line'] = line.strip()
    expobj['err'] = exc_obj

    return expobj

################################################
#   subprocessShellExecute
#       1 . MySQL
#       2.  Execuable
################################################
def subprocessShellExecute(cmd):
    retObj = {}
    p = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    out, err = p.communicate()
    if p.returncode != 0:
        # an error happened!
        err_msg = "%s. Code: %s" % (err.strip(), p.returncode)
        retObj['ret'] = False
        retObj['msg'] = err_msg  
    else:
        retObj['ret'] = True
        if len(err): # warnning
            retObj['msg'] = err
        retObj['output'] = out
    #p.kill()
    return retObj

def endProcess(removedir, f):

    if not f.closed:
        f.flush()
        f.close()
        
    if os.path.isdir(removedir):
        util.removeDir(removedir) 

def genAggregatecsv(spark, sqlquery, savepath, filename, logf):

    ret = 0
    kpidf = None
    coalesce = 8

    util.logMessage('executing query: {}'.format(sqlquery), logf)
    try:
        kpidf = spark.sql(sqlquery)
        if kpidf is None:
            ret = 1
    except:
        ret = 1
        util.logMessage('query exception:', logf)
        util.logMessage(getException(), logf)
        util.printTrace(logf)
    finally:
        if ret != 0:
            return ret

    util.logMessage('saving query results to {} ...'.format(savepath), logf)
    try:
        kpidf.coalesce(coalesce).write.csv(savepath, 
                                    header=True, 
                                    mode='overwrite', 
                                    sep=',', 
                                    dateFormat='yyyy-MM-dd', 
                                    timestampFormat='yyyy-MM-dd HH:mm:ss')
    except:
        util.logMessage('issue to save data to csv', logf)
        util.logMessage(getException(), logf)
        util.printTrace(logf)
        return 1
    else:
        if coalesce <= 1:
            util.logMessage('coalesce: {}, only one single csv'.format(coalesce), logf)
            resultcsvfile = os.path.join(os.path.dirname(savepath), "{}.csv".format(filename))
            for csvfile in glob.glob(os.path.join(savepath, "*.csv")):
                os.rename(csvfile, resultcsvfile)
            util.logMessage('final csv: {}'.format(resultcsvfile), logf)
            util.removeDir(savepath)
            return 0
        else:
            try:
                util.logMessage('coalesce: {}, union query results csvs ...'.format(coalesce), logf)
                resultcsvfile = os.path.join(os.path.dirname(savepath), "{}.csv".format(filename))
                util.logMessage('final csv: {}'.format(resultcsvfile), logf)
                resf = open(resultcsvfile, 'a')
            except:
                util.logMessage('cannot create results csv {}'.format(resultcsvfile), logf)
                return 1
            else:
                bsuccess = True
                breadheader = False
                header_line = ''
                for csvfile in glob.glob(os.path.join(savepath, "*.csv")):
                    content = []
                    csvf = None
                    try:
                        with open(csvfile, 'r') as csvf:
                            for line in csvf:
                                content.append(line)
                    except:
                        util.logMessage('ERROR: reading csv failed: {}'.format(csvfile), logf)
                        bsuccess = False
                        continue
                    else:
                        if len(content) == 0:
                            util.logMessage('WARNNING: no content in {} when creating {}'.format(csvfile, savepath), logf)
                        else:
                            if not breadheader:
                                resf.write(content[0])
                                breadheader = True
                            content.pop(0)
                            for content_line in content:
                                resf.write(content_line)
                resf.close()
                util.removeDir(savepath)
                if bsuccess:
                    return 0
                else:
                    return 1

def getdatadatetime(spark, df, view, logf):

    datetimearr = []
    numaggdate = 2
    getmaxdatesql = "select max(pk_date) as maxdate From {} group by pk_date order by pk_date DESC limit {}".format(view, numaggdate)
    
    getdatetimelistsql = "select t1.pk_date, t1.pk_market, t1.pk_hr \
From ltenokiakpi as t1, ({}) as t2 where t1.pk_date = t2.maxdate Group By t1.pk_date, t1.pk_market, t1.pk_hr \
order by t1.pk_market ASC, t1.pk_date DESC, t1.pk_hr DESC".format(getmaxdatesql)

    util.logMessage('get date time query: {}'.format(getdatetimelistsql), logf)
    try:
        datetimearr = spark.sql(getdatetimelistsql).collect()
        print datetimearr
        if len(datetimearr) <= 0:
            util.logMessage('unable to get date hour information from parquet', logf)
    except:
        util.logMessage('query exception: unable to get date time information', logf)
        util.logMessage(getException(), logf)
        util.printTrace(logf)
    finally:
        return datetimearr

def getdatadatetime2(pqfiletypedir, previousdatehrs, logf):

    datetimearr = []
    latestdate = ''
    datefdcnt = 0
    numaggdate = 1 # get latest number days
    for datefd in sorted(os.listdir(pqfiletypedir), reverse=True):
        if datefd.find("pk_date") >= 0 and datefdcnt < numaggdate:      
            datadate = datefd.split('=')[1].strip('\n').strip('\r')
            datefdcnt += 1  
            for mktfd in sorted(os.listdir(os.path.join(pqfiletypedir, datefd)), reverse=True):
                if mktfd.find("pk_market") >= 0:
                    datamkt = mktfd.split('=')[1].strip('\n').strip('\r')
                    hrfdcnt = 0
                    for hrfd in sorted(os.listdir(os.path.join(pqfiletypedir, datefd, mktfd)), reverse=True):
                        if hrfd.find("pk_hr") >= 0:
                            bAdd = True
                            if datefdcnt != 1: # not latest date only 3 hrs
                                if hrfdcnt > previousdatehrs:
                                    bAdd = False
                            if bAdd:
                                datahr = hrfd.split('=')[1].strip('\n').strip('\r')
                                datetimeobj = {}
                                datetimeobj['pk_date'] = datadate
                                datetimeobj['pk_market'] = datamkt
                                datetimeobj['pk_hr'] = datahr
                                datetimearr.append(datetimeobj)
                                hrfdcnt += 1
 
    return datetimearr

def getMarketSuffix(spark, view, market, logf):

    marketsuffix = 'unassigned'
    util.logMessage('getting market suffix for market {} ...'.format(market), logf)
    query = "select distinct(HL_Market_Suffix) as marketsuffix from {} where pk_market = '{}'".format(view, market)
    try:
        ret = spark.sql(query).collect()
        for row in ret:
            marketsuffix = row['marketsuffix']
            break
    except:
        util.logMessage('query exception:', logf)
        util.logMessage(getException(), logf)
        util.printTrace(logf)
    finally:
        util.logMessage('market suffix: {}'.format(marketsuffix), logf)
        if marketsuffix == "unassigned":
            util.logMessage("market suffix is unassigned, rename it to \"null\" for output format".format(marketsuffix), logf)
            marketsuffix = "null"
        return marketsuffix

def kpiAppregation(spark, sqlquery, pqfiletypedir, csvpath, filetype, mktuidmap, logf = None):

    df = None
    tempview = 'nokiakpi_{}'.format(filetype)
    sqlquery = sqlquery.replace('{view}', tempview)
    util.logMessage('reading parquet : {}'.format(pqfiletypedir), logf)
    
    try:
        df = spark.read.parquet(pqfiletypedir)
        if df is None:
            util.logMessage('empty df when read parquet: {}'.format(pqfiletypedir), logf)
            return 1
    except:
        util.logMessage('read parquet failed: {}'.format(pqfiletypedir), logf)
        util.logMessage(getException())
        return 1

    df = df.withColumn("hl_date", pysparksqlfunc.date_format(df['PERIOD_START_TIME'], 'yyyy-MM-dd HH:00:00'))
    df = df.withColumn("hl_date_hour", pysparksqlfunc.date_format(df['PERIOD_START_TIME'], 'yyyy-MM-dd HH:00:00'))
    df = df.withColumn("PERIOD_START_TIME", pysparksqlfunc.date_format(df['PERIOD_START_TIME'], 'yyyy-MM-dd HH:00:00'))
    df.createOrReplaceTempView(tempview)
        
    # get data date time
    datetimearr = []
    previousdatehrs = 3
    #datetimearr = getdatadatetime(spark, df, tempview, logf)
    datetimearr = getdatadatetime2(pqfiletypedir, previousdatehrs, logf)
    if len(datetimearr) <= 0:
        spark.catalog.dropTempView(tempview)
        df = None
        return 1
    
    ##################
    #
    #   get hourly and dately csv if latest date for every market
    #   get hourly csv (previousdatehrs) if not latest date for every market
    #
    ##################
    ret = 0
    finalret = 0
    kpidf = None
    lastdate = ''
    cmarket = ''
    marketchagne = False
    datechange = False
    bcreatedailycsv = False
    numhrscreated = 0
    aggcsvfdname = os.path.basename(csvpath)
    uidstr = ''
    for date in datetimearr:

        aggcsvpath = os.path.join(csvpath, filetype)
        tmpmarket = date['pk_market']

        # market change
        if cmarket != tmpmarket:
            cmarket = tmpmarket
            marketchagne = True
            bcreatedailycsv = True

            # reset lastest date
            lastdate = ''
            datechange = False

            # get market suffix
            marketsuffix = getMarketSuffix(spark, tempview, date['pk_market'], logf)
        else:
            marketchagne = False

        # date change
        tmpdate = date['pk_date']
        if lastdate == '':
            lastdate = tmpdate
        else:
            if lastdate != tmpdate:
                datechange = True # start using previousdatehrs

        # market-uid map, same market will use same uid for all file types
        bfinduid = False
        for k, v in mktuidmap.iteritems():
            if k == marketsuffix:
                uidstr = v
                bfinduid = True
                break
        if not bfinduid:
            uidstr = str(uuid.uuid1())
            mktuidmap[marketsuffix] = '{}'.format(uidstr)

        # create daily results
        if bcreatedailycsv:
            util.logMessage('{} - DAILY CSV: processing market: {} - date: {}'.format(filetype, date['pk_market'], date['pk_date']), logf)
            sqlquerydaily = sqlquery.replace("{where}", \
                "where pk_market = '{}' and pk_date = '{}'".format(date['pk_market'], date['pk_date']))
            sqlquerydaily = sqlquerydaily.replace("'unassigned' as MGR_RUN_ID", "'24-{}' AS MGR_RUN_ID".format(uidstr))
            sqlquerydaily = sqlquerydaily.replace("hl_date as", "MIN(from_unixtime(unix_timestamp(HL_DATE, 'yyyy-MM-dd'), 'yyyy-MM-dd 00:00:00')) as")
            sqlquerydaily = sqlquerydaily.replace("GROUP BY hl_date,", "GROUP BY ")
            finalcsvfilename = '{}_{}_{}_{}_{}'\
                .format(aggcsvfdname, date['pk_date'], marketsuffix.lower(), date['pk_market'].replace(" ", "-").upper(), filetype.replace("_", "-"))
            finalret += genAggregatecsv(spark, sqlquerydaily, aggcsvpath, finalcsvfilename, logf)
            
            bcreatedailycsv = False

        # create hr results
        bcreatehourly = True
        if datechange: # not latest, need to create last previousdatehrs(3) hours csv
            if numhrscreated < previousdatehrs:
                numhrscreated += 1
            else:
                bcreatehourly = False

        if bcreatehourly:
            util.logMessage('{} - HOURLY CSV: processing market: {} - date: {} - hour: {}'.format(filetype, date['pk_market'], date['pk_date'], date['pk_hr']), logf)
            sqlqueryhourly = sqlquery.replace("{where}", \
" where pk_market = '{}' and pk_date = '{}' and pk_hr = '{}'".format(date['pk_market'], date['pk_date'], str(date['pk_hr'])))
            sqlqueryhourly = sqlqueryhourly.replace("'unassigned' as MGR_RUN_ID", "'{}-{}' AS MGR_RUN_ID".format(str(date['pk_hr']).zfill(2), uidstr))
            finalcsvfilename = '{}_{}_{}_{}_{}_{}'\
                .format(aggcsvfdname, date['pk_date'], marketsuffix.lower(), date['pk_market'].replace(" ", "-").upper(), str(date['pk_hr']).zfill(2), filetype.replace("_", "-"))
            finalret += genAggregatecsv(spark, sqlqueryhourly, aggcsvpath, finalcsvfilename, logf)

    spark.catalog.dropTempView(tempview)
    kpidf = None
    df = None

    return finalret

def runKpiAggregation(spark, vendor, tech, carr, sqljsonfile, parquetpath, aggregationcsvpath):

    uid = uuid.uuid1()
    datetimearr = datetime.datetime.now().strftime('%Y%m%d %H%M%S%f').split(' ')
    wfolder='ttskpiagg_{}_{}_{}_{}_{}'.format(vendor.upper(), tech.upper(), datetimearr[0], datetimearr[1], carr.upper())
    csvpath = os.path.join(aggregationcsvpath, wfolder)
    if os.path.isdir(csvpath):
        util.removeDir(csvpath)
    try:
        os.mkdir(csvpath)
    except:
        util.logMessage("create csv results directory failed: {}".format(csvpath))
        util.logMessage(getException())
        return 1

    # log file
    logf = None
    logfile = os.path.join(csvpath, wfolder + ".log")
    try:
        logf = open(logfile, "w", 0) 
    except IOError, e:
        util.logMessage(e.errno)
        util.logMessage(getException())
        pass
            
    util.logMessage('start kpi aggregation process ...', logf)
    util.logMessage('reading sql json file: {}'.format(sqljsonfile), logf)
    try:
        with open(sqljsonfile, 'r') as json_data:
            sqljson = json.load(json_data)

        if 'features' not in sqljson:
            if len(sqljson['features']) <= 0:
                util.logMessage('sql json format incorrect (not feature or feature is empty)', logf)
                return 1
    except:
        util.logMessage(getException())
        return 1

    totaldir = 0
    findsql = 0
    aggsuccess = 0
    mktuidmap = {}
    for filetype in os.listdir(parquetpath):
        util.logMessage('============== file type: {} =============='.format(filetype), logf)
        pqfiletypedir = os.path.join(parquetpath, filetype)
        if os.path.isdir(pqfiletypedir):
            totaldir += 1
            util.logMessage('file type folder: {}'.format(pqfiletypedir), logf)
            arr = filetype.split('_')
            arr.insert(1, 'nokia')
            sqlname = '_'.join(arr)

            bfind = False
            for sqlobj in sqljson['features']:
                if sqlobj['name'] == sqlname and 'sql' in sqlobj:
                    if len(sqlobj['sql']) > 0:
                        findsql += 1
                        bfind = True
                        util.logMessage('find sql type {} in json'.format(sqlname), logf)
                        sqlquery = ''
                        if sqlobj is None:
                            util.logMessage("single query testing now", logf)
                        else:
                            for sql in sqlobj['sql']:
                                if "SELECT" in sql and "GROUPBY" in sql:
                                    sqlquery = "SELECT {} FROM {} {} GROUP BY {}".format(sql['SELECT'], '{view}', '{where}', sql['GROUPBY'])
                        if sqlquery == '':
                            util.logMessage('empty sql query from: {}'.format(sqlobj), logf)
                            continue
                        else:
                            ret = kpiAppregation(spark, sqlquery, pqfiletypedir, csvpath, filetype, mktuidmap, logf)
                            if ret == 0:
                                aggsuccess += 1
                                util.logMessage("aggregate type: {} SUCCESS".format(filetype), logf)
                            else:
                                util.logMessage("aggregate type: {} FAILED".format(filetype), logf)
                            break

            if not bfind:
                util.logMessage("no sql object definded: {} for {}".format(sqlname, pqfiletypedir), logf)

    util.logMessage("=== Stats ===")
    util.logMessage(" # known types (parquets) : {} of {} ".format(findsql, totaldir), logf)
    util.logMessage(" # aggregation success    : {} of {} ".format(aggsuccess, findsql), logf)

    '''
    util.logMessage('testing ..............')
    pqfiletypedir = "/mnt/nfskpi/wyang/ttskpiraw/lte-nokia/parquet/lte_isys_ho_utran_nb_sum"
    testquery = "select 'unassigned' as MGR_RUN_ID, min(oss) as oss, min(PERIOD_START_TIME) as PERIOD_START_TIME\
, sum(period_duration) as period_duration, mo_dn_source as mo_dn, mo_dn2 as cell\
, sum(isys_ho_utran_att_nb) as isys_ho_utran_att_nb, sum(isys_ho_utran_fail_nb) as isys_ho_utran_fail_nb\
, sum(isys_ho_utran_srvcc_att_nb) as isys_ho_utran_srvcc_att_nb, sum(isys_ho_utran_srvcc_fail_nb) as isys_ho_utran_srvcc_fail_nb\
, hl_date_hour, hl_date, hl_sector, 'unassigned' as hl_sectorlayer, hl_site, hl_cluster, hl_area, hl_market \
    From {view} {where} Group By mo_dn_source, mo_dn2, hl_date, hl_date_hour, hl_sector, hl_site, hl_cluster, hl_area, hl_market"
    ret = kpiAppregation(spark, testquery, pqfiletypedir, csvpath, 'lte_isys_ho_utran_nb_sum', logf)
    '''

    util.logMessage("")
    util.logMessage("packaging results ... ", logf)
    packResults(csvpath, mktuidmap, logf)
    
    return 0

def packResults(csvpath, mktuidmap, logf):

    mainoutputdir = os.path.dirname(csvpath)
    mainzipname = os.path.basename(csvpath)
    results = os.path.join(csvpath, "*.csv")
    print csvpath
    for resultfile in glob.glob(results):
        resultfilenamearr = os.path.basename(resultfile).split('_')
        pkdate = resultfilenamearr[6]
        suffix = resultfilenamearr[7]
        marketname = resultfilenamearr[8]
        if not os.path.isdir(os.path.join(csvpath, '{}_{}_{}'.format(pkdate, suffix.lower(), marketname))):
            os.mkdir(os.path.join(csvpath, '{}_{}_{}'.format(pkdate, suffix.lower(), marketname)))
        util.copyFile(resultfile, os.path.join(csvpath, '{}_{}_{}'.format(pkdate, suffix, marketname)), False, True)

    for root, dirs, files in os.walk(csvpath):
        for resultdir in dirs:
            for logfile in glob.glob(os.path.join(csvpath, "*.log")):
                util.copyFile(logfile, os.path.join(root, resultdir))

            # get market run uid
            uidstr = ''
            resultdirarr = resultdir.split('_')
            if len(resultdirarr) >= 3:
                resultmkt = resultdirarr[1]
                for k, v in mktuidmap.iteritems():
                    if k == resultmkt:
                        uidstr = v
                        break
            if uidstr == '':
                continue

            # same makret for different file type may have different max hr, get max hr here
            maxhr = -1
            for resulttxt in glob.glob(os.path.join(root, resultdir, "*.csv")):
                resulttxtarr = os.path.basename(resulttxt).split('_')
                if len(resulttxtarr) >= 11:
                    resulthr = int(resulttxtarr[9])
                    # init here
                    if maxhr == -1:
                        maxhr = 0
                    if resulthr > maxhr:
                        maxhr = resulthr
            if maxhr == -1:
                continue

            # zip results for loader, hr-uid
            # ttskpiagg_20170713_015006710_NOKIA_LTE_TMO_2017-03-29_spk_SPOKANE_12-4baccf30-67a8-11e7-8f28-000c295b3aae.tgz
            tmpgzfile = os.path.join(mainoutputdir, '{}_{}_{}-{}.tgz.tmp'.format(mainzipname, resultdir, str(maxhr).zfill(2), uidstr))
            finalgzfile = tmpgzfile.replace(".tmp", "")
            cmd = 'tar -C {} -zcvf {} .'.format(os.path.join(root, resultdir), tmpgzfile)
            util.logMessage('archive cmd: {}'.format(cmd), logf)
            ret = subprocessShellExecute(cmd)
            if ret['ret']:
                util.logMessage('temp results archived: {}'.format(tmpgzfile), logf)
                try:
                    util.logMessage('rename {} to {}'.format(tmpgzfile, finalgzfile), logf)
                    os.rename(tmpgzfile, finalgzfile)
                except:
                    util.logMessage('rename {} to {} failed'.format(tmpgzfile, finalgzfile), logf)
                    util.logMessage(getException(), logf)
                    continue
            else:
                util.logMessage('failed to archive input file', logf)
                util.logMessage('error: {}'.format(ret['msg']), logf)
                continue

    endProcess(csvpath, logf)
    #util.removeDir(csvpath)

def packParserResults(wfolderpath, archivepath, logf):
    wfolder = os.path.basename(wfolderpath)
    wfolderdir = os.path.dirname(wfolderpath)
    archivegzpath = os.path.join(archivepath, wfolder + ".tgz")
    util.logMessage('archiving sequence input file to {}'.format(archivegzpath), logf)
    cmd = 'tar -C {} -zcvf {} {}'.format(wfolderdir, archivegzpath, wfolder)
    util.logMessage('archive cmd: {}'.format(cmd), logf)
    ret = subprocessShellExecute(cmd)
    if ret['ret']:
        util.logMessage('input archived', logf)
    else:
        util.logMessage('failed to archive input file', logf)
        util.logMessage('error: {}'.format(ret['msg']), logf)
        
def convertColumn(df, name, new_type):
    newdf = df.withColumnRenamed(name, "swap")
    newdf = newdf.withColumn(name, newdf.swap.cast(new_type)).drop("swap")
    return newdf

def readLookupParquet(spark, lookupview, celllookuppk, logf):

    util.logMessage("reading lookup parquet: {}".format(celllookuppk), logf)

    dfLookup = None
    try:
        dfLookup = spark.read.parquet(celllookuppk)
        dfLookup.createOrReplaceTempView(lookupview)
    except:
        util.printTrace(logf)
    finally:
        return dfLookup

def createParquetFile(spark, vendor, tech, filetypegroup, celllookuppk, parquetpath, logf):

    lookupview = '{}{}lookup'.format(tech.lower(), vendor.lower())
    dfLookup = readLookupParquet(spark, lookupview, celllookuppk, logf)
    if dfLookup is None:
        util.logMessage('failed to read lookup parquet: {}'.format(celllookuppk), logf)
        return 1

    util.logMessage('creating parquet file ...', logf)
    
    sc = spark.sparkContext

    for fg in filetypegroup['groups']:

        schemaJson = ''
        try:
            with open(fg['schemapath']) as json_data:
                schemaJson = json.load(json_data)
        except:
            util.logMessage(getException(), logf)
            continue

        try:
            schema = StructType([StructField.fromJson(item) for item in schemaJson])
        except:
            util.logMessage(getException(), logf)
            continue
    
        bhasparquet = False
        parquettype = fg['type']
        parquettypepath = os.path.join(parquetpath, parquettype)
        if os.path.isdir(parquettypepath):
            bhasparquet = True
    
        fcount = 0
        loadfactor = 10
        uniondf = None
        for fn in fg['files']: 
            df = None
            util.logMessage('reading file: {}'.format(fn), logf)
        
            try:
                df = spark.read.csv(fn, schema, ignoreLeadingWhiteSpace = True, sep = '|' \
                    , ignoreTrailingWhiteSpace = True, header = True, timestampFormat = 'yyyy-MM-dd HH:mm')
            except:
                util.logMessage('failed to read {}'.format(fn), logf)
                util.printTrace(logf)
                continue

            if df is None:
                util.logMessage('empty dataframe from: {}'.format(fn), logf)
                continue

            fcount += 1

            if uniondf is None:
                uniondf = df
            else:
                uniondf = uniondf.union(df)
                
                if fcount % loadfactor == 0:
                    if not bhasparquet:
                        ret = saveParquetFile(spark, vendor, tech, uniondf, 'overwrite', lookupview, parquettype, parquettypepath, logf)
                        if ret == 0:
                            bhasparquet = True    
                    else:
                        ret = saveParquetFile(spark, vendor, tech, uniondf, 'append', lookupview, parquettype, parquettypepath, logf)
                    uniondf = None

        if uniondf is not None:
            if not bhasparquet:
                ret = saveParquetFile(spark, vendor, tech, uniondf, 'overwrite', lookupview, parquettype, parquettypepath, logf)
                if ret == 0:
                    bhasparquet = True
            else:
                ret = saveParquetFile(spark, vendor, tech, uniondf, 'append', lookupview, parquettype, parquettypepath, logf)
        uniondf = None
        
    return 0

def saveParquetFile(spark, vendor, tech, df, mode, lookupview, parquettype, parquettypepath, logf):

    joindf = None
    df.createOrReplaceTempView(parquettype)
    lookupquery = "SELECT k.*\
, IFNULL(l.CELL,'unassigned') as HL_Sector\
, IFNULL(l.SITE,'unassigned') as HL_Site\
, IFNULL(l.MARKET,'unassigned') as HL_Market\
, IFNULL(l.CLUSTER,'unassigned') AS HL_Cluster\
, IFNULL(l.MARKET_SUFFIX,'unassigned') AS HL_Market_Suffix\
, IFNULL(l.AREA,'unassigned') AS HL_Area \
from {} k left join {} l on k.cell_id_1 = l.CELL_UID AND l.TECH = '{}' AND l.VENDOR = '{}'".format(parquettype, lookupview, tech.upper(), vendor.upper())
    
    try:
        util.logMessage('lookup query: {}'.format(lookupquery), logf)
        joindf = spark.sql(lookupquery)
    except:
        util.logMessage('save {} parquet FAILED due to lookup failed'.format(parquettype), logf)
        util.logMessage('exception:', logf)
        util.logMessage(getException(), logf)
        util.printTrace(logf)
        return 1

    if joindf is None:
        util.logMessage('No cell name matched when saving {} parquet'.format(parquettype), logf)
        return 2

    # add key cols
    joindf = joindf.withColumn("pk_date", pysparksqlfunc.date_format(df['PERIOD_START_TIME'], 'yyyy-MM-dd'))
    joindf = joindf.withColumn("pk_market", joindf['HL_Market'])
    joindf = joindf.withColumn("pk_hr", pysparksqlfunc.date_format(df['PERIOD_START_TIME'], 'HH'))
    
    try:
        joindf.write \
            .partitionBy('pk_date', 'pk_market', 'pk_hr') \
            .mode(mode) \
            .parquet(parquettypepath, compression='gzip')
    except:
        util.logMessage('save {} parquet FAILED'.format(parquettype), logf)
        util.printTrace(logf)
        return 1
    else:
        util.logMessage('save {} parquet SUCCESS'.format(parquettype), logf)

    return 0

def groupFileType(inputpath, schemapath, wfolderpath, logf):

    util.logMessage('grouping file types ...', logf)

    inputdir = os.path.dirname(inputpath)
    filetypegroup = {}
    filetypegroup['filecount'] = 0
    filetypegroup['notype'] = 0
    filetypegroup['groups'] = []
    filetypegroup['noschemagroups'] = []

    # move input files first
    for fn in glob.glob(inputpath):
        filename = os.path.basename(fn)
        os.rename(fn, os.path.join(wfolderpath, filename))

    # grouping files
    wfolderpathfiles = os.path.join(wfolderpath, '*')
    for fn in glob.glob(wfolderpathfiles):

        filegroupobj = {}
        filename = os.path.basename(fn)
        filenamenoext = os.path.splitext(filename)[0]
        filenamearr = filenamenoext.split('_')
        try:
            filetype = '_'.join(filenamearr[6:])
        except:
            filetypegroup['notype'] += 1
            util.logMessage('cannot get file type: {}'.format(fn), logf)
            continue
        
        bfindgroup = False
        for idx, fg in enumerate(filetypegroup['groups']):
            if fg['type'] == filetype:
                fg['count'] += 1
                fg['files'].append(fn)
                filetypegroup['groups'][idx] = fg
                bfindgroup = True
                break

        # add new file type
        if not bfindgroup:
            filetypeobj = {}

            schemaarr = filetype.split('_')
            schemaarr.insert(1, 'nokia')
            schemaarr.insert(len(schemaarr)+1, 'schema')
            targetschemafile = '_'.join(schemaarr) + '.json'
            bfindschema = False
            schemajson = ''
            for schemajson in os.listdir(schemapath):
                if schemajson.endswith(".json"):
                    if schemajson == targetschemafile:
                        bfindschema = True
                        break
                    
            filetypeobj['type'] = filetype
            filetypeobj['count'] = 1
            filetypeobj['schema'] = ''
            filetypeobj['files'] = []
            
            if bfindschema:
                filetypeobj['schema'] = targetschemafile
                filetypeobj['schemapath'] = os.path.join(schemapath, targetschemafile)
                filetypeobj['files'].append(fn)
                
                filetypegroup['groups'].append(filetypeobj)
            else:
                unknownfolderdir = os.path.join(inputdir, "unknown")
                if not os.path.isdir(unknownfolderdir):
                    os.mkdir(unknownfolderdir)
                
                os.rename(fn, os.path.join(unknownfolderdir, filename))
                util.logMessage('mv unknow file type {} to {}'.format(fn, unknownfolderdir), logf)
                # ignore the file information that cannot find schema now
                #filetypegroup['noschemagroups'].append(filetypeobj)
            

        filetypegroup['filecount'] += 1
            
    return filetypegroup

def main():

    root = os.path.dirname(os.path.realpath(__file__))
    
    argvs = len(sys.argv)
    try:
        funcid = sys.argv[1]
    except:
        util.logMessage("incorrect arguments - no function id")
        return 1

    if funcid == '1':
        '''
        spark-submit --master mesos://zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos
        --driver-memory 512M --executor-memory 916M --total-executor-cores 8
        /home/tts/test_results/code/umts-nokia/umts_nokia_aggregator.py 1 NOKIA UMTS TMO
        /mnt/nfsi01/ttskpiraw/umts-nokia/aggregatorInput
        "/mnt/nfsi01/ttskpiraw/umts-nokia/aggregatorInput/new_set_sample/*.txt" /mnt/nfsi/ttskpicellex/CellExFromSinfo.pqz
        /mnt/nfsi01/ttskpiraw/umts-nokia/parquet /mnt/nfsi01/ttskpiraw/umts-nokia/dbloaderInput
        '''
        if argvs < 9:
            util.logMessage("incorrect arguments")
            return 1

        vendor = sys.argv[2]
        tech = sys.argv[3]
        carr = sys.argv[4]
        aggregatorInput = sys.argv[5]
        parserresultspath = sys.argv[6]
        celllookuppk = sys.argv[7]
        parquetpath = sys.argv[8]
        aggregationcsvpath = sys.argv[9]
        datetimearr = datetime.datetime.now().strftime('%Y%m%d %H%M%S%f').split(' ')

        appName = '{} {} kpi aggregation'.format(vendor.lower(), tech.lower())

        stagingpath = os.path.join(aggregatorInput, "staging")
        if not os.path.isdir(stagingpath):
            try:
                os.mkdir(stagingpath)
            except:
                util.logMessage("create staging directory failed: {}".format(stagingpath))
                util.logMessage(getException())
                return 1
            
        # create working directory
        uid = uuid.uuid1()
        #wfolder='{}'.format(str(uid))
        wfolder='ttskpiraw_{}_{}_{}_{}_{}'.format(vendor.upper(), tech.upper(), datetimearr[0], datetimearr[1], carr.upper())
        wfolderpath = os.path.join(aggregatorInput, "staging", wfolder)
        if os.path.isdir(wfolderpath):
            util.removeDir(wfolderpath)
        try:
            os.mkdir(wfolderpath)
        except:
            util.logMessage("create working directory failed: {}".format(wfolderpath))
            util.logMessage(getException())
            return 1

        # log file
        logf = None
        logfile = os.path.join(wfolderpath, wfolder + ".log")
        try:
            logf = open(logfile, "w", 0) 
        except IOError, e:
            util.logMessage(e.errno)
            util.logMessage(getException())
            pass

        util.logMessage("=== {}: results creation - full ===".format(appName), logf)

        archivepath = os.path.join(aggregatorInput, "archive")
        if not os.path.isdir(archivepath):
            util.logMessage("archive path does not exist {}".format(archivepath), logf)
            try:
                util.logMessage("creating archive path: {}".format(archivepath), logf)
                os.mkdir(archivepath)
            except:
                util.logMessage("create archive directory failed: {}".format(archivepath))
                util.logMessage(getException())
                return 1

        if not os.path.isdir(parserresultspath):
            util.logMessage("creating parquet root path: {}".format(parserresultspath), logf)
            os.mkdir(parserresultspath)
           
        schemapath = os.path.join(root, 'schema')
        sqljsonfile = os.path.join(root, 'sql', '{}_{}_sql.json'.format(tech.lower(), vendor.lower()))
            
        # group file type
        filetypegroup = groupFileType(parserresultspath, schemapath, wfolderpath, logf)
        if filetypegroup['filecount'] <= 0 or len(filetypegroup['groups']) <= 0:
            util.logMessage("no input file can be processed in: {}".format(parserresultspath), logf)
            endProcess(wfolderpath, logf)
            return 1            

        util.logMessage(json.dumps(filetypegroup, indent=4), logf)

        # create parquet file
        spark = SparkSession \
            .builder \
            .appName(appName) \
            .getOrCreate()

        ret = createParquetFile(spark, vendor, tech, filetypegroup, celllookuppk, parquetpath, logf)

        # archive parser output
        util.logMessage("packing parser results ...", logf)
        packParserResults(wfolderpath, archivepath, logf)
        endProcess(wfolderpath, logf)

        # aggregate results
        ret = runKpiAggregation(spark, vendor, tech, carr, sqljsonfile, parquetpath, aggregationcsvpath)
        spark.stop()

    elif funcid == '3':
        '''
        spark-submit --master mesos://zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos
         --driver-memory 512M --executor-memory 916M --total-executor-cores 8
         /home/imnosrf/test_results/code/lte-nokia/lte_nokia_aggregator.py 3 NOKIA LTE TMO /mnt/nfskpi/wyang/ttskpiraw/lte-nokia/parquet
         /mnt/nfskpi/wyang/ttskpiraw/lte-nokia/aggregation
        '''
        if argvs < 7:
            util.logMessage("incorrect arguments")
            return 1

        vendor = sys.argv[2]
        tech = sys.argv[3]
        carr = sys.argv[4]
        parquetpath = sys.argv[5]
        aggregationcsvpath = sys.argv[6]

        appName = '{} {} kpi aggregation'.format(vendor.lower(), tech.lower())
        
        # aggregation
        spark = SparkSession \
            .builder \
            .appName(appName) \
            .getOrCreate()

        util.logMessage("=== {}: results creation - aggregation ===".format(appName))

        sqljsonfile = os.path.join(root, 'sql', '{}_{}_sql.json'.format(tech.lower(), vendor.lower()))
        ret = runKpiAggregation(spark, vendor, tech, carr, sqljsonfile, parquetpath, aggregationcsvpath)

        spark.stop()
        
    else:
        util.logMessage("unknow function id")
        return 1
    
    return ret;

if __name__ == "__main__":

    start = time.time()
    ret = main()
    
    end = time.time()
    elapsed = end - start
    util.logMessage('exit: {}, process time: {}'.format(ret, elapsed))

    # remove lock, turn on later
    #util.removeDir('/tmp/lte_nokia_aggregator.lock')
    
    os._exit(ret)

