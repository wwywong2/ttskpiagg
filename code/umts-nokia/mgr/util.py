#!/usr/bin/python
import os, glob, uuid
import sys, subprocess, json
import socket
import time # This is required to include time module
import random
import shutil

import linecache # for getException()

import logging
from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler



# setup logger
mainLog = None



######## json_reader ########
def json_reader(jsonFile):
    import json
    
    with open(jsonFile) as data_file:    
        data = json.load(data_file)

    return data

######## fileExist ########
def fileExist(fileFullPath):
    import os
    return os.path.isfile(fileFullPath)

######## removeDir (recursively) ########
def removeDir(path):
    import shutil
    return shutil.rmtree(path)

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

def loggerSetup(loggerName='', outfile='', defaultLvl=logging.WARNING, timed=False, custFmtStr=''):

    global mainLog

    hostname = socket.gethostname()
    fmtStr = '[%%(asctime)s.%%(msecs)03d @%s] %%(name)s %%(levelname)s: %%(message)s' % hostname
    if custFmtStr != '':
        fmtStr = custFmtStr

    # set formatter
    formatter = logging.Formatter(fmtStr, datefmt='%Y-%m-%d %H:%M:%S')

    # set handler - stdout
    hdlr_stdout = logging.StreamHandler(sys.stdout)
    hdlr_stdout.setLevel(defaultLvl)
    hdlr_stdout.setFormatter(formatter)

    hdlr_rfile = None
    hdlr_trfile = None

    if outfile != '':
        if not timed:
            # set handler - rotating file
            hdlr_rfile = RotatingFileHandler(outfile, maxBytes=1024*1024, backupCount=2)
            hdlr_rfile.setLevel(defaultLvl)
            hdlr_rfile.setFormatter(formatter)
        else:
            # set handler - timed rotating file - S M H D 'W0'-'W6'(mon-sun) 'midnight' 
            hdlr_trfile = TimedRotatingFileHandler(outfile, when='D', interval=1, backupCount=4)
            hdlr_trfile.setLevel(defaultLvl)
            hdlr_trfile.setFormatter(formatter)
    

    # set logger
    if loggerName == '':
        logger = logging.getLogger(__name__)
    else:
        logger = logging.getLogger(loggerName)
    logger.setLevel(defaultLvl)
    logger.addHandler(hdlr_stdout) # output to stdout
    if outfile != '':
        if not timed:
            logger.addHandler(hdlr_rfile) # output to rotating file
        else:
            logger.addHandler(hdlr_trfile) # output to timed rotating file

    if mainLog is None: # only set global the first time this func is called
        mainLog = logger # save to global

    return logger

# logic:
# 1.) if file are specified, output to file and stdout (old method)
# 2.) if logger are specified, output to logger
# 3.) if mainLog are defined (global), output to mainLog
# 4.) if all of above are not specified, output to stdout (old method) 
def logMessage(message, logFilePt=None, logger=None, level=logging.INFO):

    global mainLog

    if logFilePt is not None:
        logMessageInt(message, logFilePt)
    elif logger is not None:
        logMessageLog(message, logger, level)
    elif mainLog is not None:
        logMessageLog(message, mainLog, level)
    else:
        logMessageInt(message)

# print message without logger
def logMessageInt(message, logFilePt=None):

    hostname = socket.gethostname()
    msg = "[%s @%s] %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), hostname, message)

    if (logFilePt is not None):

        try:
            logFilePt.write("%s\n" % msg)
            logFilePt.flush()
        except:
            return False

    print (msg)
    sys.stdout.flush() # flush buffer before executing next line

    return True

# print message with logger
def logMessageLog(message='', logger=None, level=logging.INFO):

    if logger is None: # safeguard
        return

    logger.log(level, message)
    # WES_TEST: do we need flush handler?
    for hdlr in logging.root.handlers:
       hdlr.flush()
   

def fileLineCount(filename):
    f = open(filename)                  
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.read # loop optimization

    buf = read_f(buf_size)
    while buf:
        lines += buf.count('\n')
        buf = read_f(buf_size)

    f.close()
    return lines

def checkRemoteFileExist(serverAddr, serverUsr, serverPwd, remoteFile):
    #sshpass -p "$webServerPwd" ssh $webServerUser@$webServer "test -e $website_hostats_file_path"
    retObj = {}
    '''
    cmd = ['sshpass', '-p', '{}'.format(serverPwd), 'ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', \
           '{}@{}'.format(serverUsr, serverAddr), 'test -e {}'.format(remoteFile)]
    '''
    cmd = ['sshpass', '-p', '{}'.format(serverPwd), 'ssh', '{}@{}'.format(serverUsr, serverAddr), 'test -e {}'.format(remoteFile)]
    p = subprocess.call(cmd)
    if p == 0:
        retObj['ret'] = True
    else:
        retObj['ret'] = False
    retObj['cmd'] = cmd
    return retObj

def copyRemoteFile(serverAddr, serverUsr, serverPwd, remoteFile, destination):
    # sshpass -p "$webServerPwd" scp $webServerUser@$webServer:"$website_hostats_file_path" "${dir_db}${packageFolder}/"
    retObj = {}
    '''
    cmd = ['sshpass', '-p', '{}'.format(serverPwd), 'scp', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', \
           '{}@{}:{}'.format(serverUsr, serverAddr, remoteFile), '{}'.format(destination)]
    '''
    cmd = ['sshpass', '-p', '{}'.format(serverPwd), 'scp', '{}@{}:{}'.format(serverUsr, serverAddr, remoteFile), '{}'.format(destination)]
    p = subprocess.call(cmd)
    if p == 0:
        retObj['ret'] = True
    else:
        retObj['ret'] = False
    retObj['cmd'] = cmd
    return retObj

def copyFileToRemote1(serverAddr, serverUsr, serverPwd, localFile, remoteDestination):
    # sshpass -p "$webServerPwd" scp $webServerUser@$webServer:"$website_hostats_file_path" "${dir_db}${packageFolder}/"
    retObj = {}
    '''
    cmd = ['sshpass', '-p', '{}'.format(serverPwd), 'scp', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', \
           '{}'.format(localFile), '{}@{}:{}'.format(serverUsr, serverAddr, remoteDestination)]
    '''
    cmd = ['sshpass', '-p', '{}'.format(serverPwd), 'scp', '-r', '%s' % localFile, '{}@{}:{}'.format(serverUsr, serverAddr, remoteDestination)]
    p = subprocess.call(cmd)
    retObj['retCode'] = p
    if p == 0:
        retObj['ret'] = True
    else:
        retObj['ret'] = False
    retObj['cmd'] = cmd

    return retObj

def copyFileToRemote2(serverAddr, serverUsr, serverPwd, localFile, remoteDestination):
    # sshpass -p "$webServerPwd" scp $webServerUser@$webServer:"$website_hostats_file_path" "${dir_db}${packageFolder}/"
    retObj = {}
    '''
    cmd = ['sshpass', '-p', '{}'.format(serverPwd), 'scp', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', \
           '{}'.format(localFile), '{}@{}:{}'.format(serverUsr, serverAddr, remoteDestination)]
    '''
    cmd = 'sshpass -p %s scp -r %s %s@%s:%s' % (serverPwd, localFile, serverUsr, serverAddr, remoteDestination)
    retObj = subprocessShellExecute(cmd)
    return retObj

def renameRemoteFile(serverAddr, serverUsr, serverPwd, remoteFile, remoteFileNew):
    #sshpass -p "$webServerPwd" ssh $webServerUser@$webServer mv remoteFile remoteFileNew
    retObj = {}
    cmd = ['sshpass', '-p', '{}'.format(serverPwd), 'ssh', '{}@{}'.format(serverUsr, serverAddr), 'mv {} {}'.format(remoteFile, remoteFileNew)]
    p = subprocess.call(cmd)
    if p == 0:
        retObj['ret'] = True
    else:
        retObj['ret'] = False
    retObj['cmd'] = cmd
    return retObj

def renameRemoteFile2(serverAddr, serverUsr, serverPwd, remoteFile, remoteFileNew):
    #sshpass -p "$webServerPwd" ssh $webServerUser@$webServer mv remoteFile remoteFileNew
    retObj = {}
    '''
    cmd = ['sshpass', '-p', '{}'.format(serverPwd), 'ssh', '{}@{}'.format(serverUsr, serverAddr), 'mv {} {}'.format(remoteFile, remoteFileNew)]
    '''
    cmd = 'sshpass -p %s ssh %s@%s mv %s %s' % (serverPwd, serverUsr, serverAddr, remoteFile, remoteFileNew)
    retObj = subprocessShellExecute(cmd)
    return retObj

################################################
#   subprocessShellExecute
#       1 . MySQL
#       2.  Execuable
################################################
def subprocessShellExecute(cmd):
    retObj = {}
    retObj['cmd'] = cmd
    p = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    out, err = p.communicate()
    if p.returncode != 0:
        # an error happened!
        err_msg = "%s. Code: %s" % (err.strip(), p.returncode)
        retObj['ret'] = False
        retObj['retCode'] = p.returncode
        retObj['errmsg'] = err_msg
        retObj['outmsg'] = out  
    else:
        retObj['ret'] = True
        retObj['retCode'] = p.returncode
        if len(err): # warnning
            retObj['errmsg'] = err
        else:
            retObj['errmsg'] = ''
        retObj['outmsg'] = out
    #p.kill()
    return retObj    

######## copyFile ########
def copyFile(srcFile, destPath, createIfNotExist = False, move = False):
    import os
    import shutil
    
    if not os.path.isfile(srcFile):
        return False
    
    if createIfNotExist:
        if not os.path.isdir(destPath):
            os.mkdir(destPath)
    try:
        if move:
            shutil.move(srcFile, destPath)
        else:
            shutil.copy(srcFile, destPath)
        return True
    except:
        return False
    
######## get_ini_value ######## 
def get_ini_value(settingFilePath, section, option, default = ''):
    import ConfigParser
    value = ''
    if not os.path.isfile(settingFilePath):
        value = default
    else:
        config = ConfigParser.ConfigParser()
        config.read(settingFilePath)

    if config is None:
        value = default
    else:
        try:
            value = config.get(section, option)
        except ConfigParser.NoSectionError:
            value = default
        except ConfigParser.NoOptionError:
            value = default
        except:
            value = default
            
    return value

######## getOS ########
#   return:
#       linux
#       darwin - Mac
#       win32  - windows (x86/x64)
#######################
def getOS():
    from sys import platform as osName
    
    if osName == "linux" or osName == "linux2":
        osName = "linux"
    
    return osName
    
######## getNowDateTimeString ########  
def getNowDateTimeString(getMillisec = False):
    import datetime
    if getMillisec:
        return datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
    else:
        return datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    
######## zip - zipIsZipFile ########    
def zipIsZipFile(fn):
    import zipfile
    return zipfile.is_zipfile(fn)

######## zip - zipReadFile ########
######## read file in zip without extract
def zipReadFile(zipFile, fn):
    import os
    import zipfile

    content = ''
    if not zipIsZipFile(zipFile):
        return content
    
    zipObj = zipfile.ZipFile(zipFile, "r")
    content = zipObj.read(fn)
       
    return content

######## zip - zipExtract ########  
def zipExtract(zipFile, extractFn = '', outpath = ''):
    import os
    import zipfile
    
    if not zipIsZipFile(zipFile):
        return False
    zipObj = zipfile.ZipFile(zipFile, "r")

    if not outpath:
        outpath = os.path.dirname(zipFile)
    
    if not extractFn:
        try:
            zipObj.extractall(outpath)
        except:
            return False
    else:
        try:
            zipObj.extract(extractFn, outpath)
        except:
            return False
       
    return True

######## zipFolder ########
def zipFolder(zipFileFullPath, folderPath, hasBaseFolder = False):
    import sys
    import os
    import zipfile
    
    retObj = {}
    
    parentFolder = os.path.dirname(folderPath)
    baseFolder = os.path.basename(folderPath)
    rootlen = len(folderPath) + 1
    
    # Retrieve the paths of the folder contents.
    contents = os.walk(folderPath)
    try:
        zipFile = zipfile.ZipFile(zipFileFullPath, 'w', zipfile.ZIP_DEFLATED)
        for root, folders, files in contents:
            for fileName in files:
                absolutePath = os.path.join(root, fileName)
                fn = absolutePath[rootlen:]
                if hasBaseFolder:
                    fn = os.path.join(baseFolder, fn) 
                zipFile.write(absolutePath, fn)
    except IOError, message:
        retObj['ret'] = False
        retObj['msg'] = message
        pass
    except OSError, message:
        retObj['ret'] = False
        retObj['msg'] = message
        pass
    except zipfile.BadZipfile, message:
        retObj['ret'] = False
        retObj['msg'] = message
        pass
    finally:
        retObj['ret'] = True
        
    return retObj
   
def getTokenInfo(filePath, bInZipFile = False):
    retObj = {}
    tokenContent = ''

    if bInZipFile:
        tokenContent = zipReadFile(filePath, 'Token.txt')
    else:
        with open(filePath, 'r') as fcontent:
            tokenContent = fcontent.read()
            
    if tokenContent == '':
        retObj['msg'] = "No information in Token.txt"
        retObj['ret'] = False
        return retObj

    # safeguard for token file coming from windows
    tokenContent = tokenContent.replace('\r\n', '\n')

    # parse token.txt
    retObj['AUDITTYPE'] = 0
    retObj['dateDiff'] = 3
    for line in tokenContent.split('\n'):
        if line.find('GENERALINFO')>=0:
            try:
                lineList = line.strip('\n').strip('\r').strip(' ').split(':')[1].split('|')
                retObj['vendor'] = lineList[1]
                retObj['recDate'] = lineList[2]
                retObj['carr'] = lineList[3]
                retObj['region'] = lineList[4]
                retObj['mktToken'] = lineList[5]
                retObj['mktDesc'] = lineList[6]
                retObj['techToken'] = lineList[7]
            except:
                retObj['msg'] = 'Incorrect Token.txt => GENERALINFO'
                retObj['ret'] = False
                return retObj
                
        if line.find('DBCONN')>=0:
            try:
                lineList = line.strip('\n').strip('\r').strip(' ').split(':')[1].split('|')
                retObj['dbHost'] = lineList[0]
                retObj['dbUser'] = lineList[1]
                retObj['dbPwd'] = lineList[2]
                retObj['dbSchema'] = lineList[3]
                retObj['dbStatusTable'] = lineList[4]
            except:
                retObj['msg'] = 'Incorrect Token.txt => DBCONN'
                retObj['ret'] = False
                return retObj
                
        if line.find('LTEPLANAUDIT')>=0:
            try:
                lineList = line.strip('\n').strip('\r').strip(' ').split(':')[1].split('|')
                if len(lineList)<1:
                    retObj['msg'] = 'LTEPLANAUDIT information in Token.txt is incorrect'
                    retObj['ret'] = False
                    return retObj
                else:
                    retObj['dbAuditTable'] = lineList[0]
                    if len(lineList)>=2:
                        optStr = lineList[1].split('=')
                        if optStr[0] == 'opt':
                            if int(optStr[1]) == 1:
                                 retObj['AUDITTYPE'] = 1
            except:
                retObj['msg'] = 'Incorrect Token.txt => LTEPLANAUDIT'
                retObj['ret'] = False
                return retObj

        if line.find('LTEPLAN')>=0:
            try:
                lineList = line.strip('\n').strip('\r').strip(' ').split(':')[1].split('|')
                retObj['dbPlanTable'] = lineList[0]
            except:
                retObj['msg'] = 'Incorrect Token.txt => LTEPLAN'
                retObj['ret'] = False
                return retObj
            
        if line.find('LTECRSFDR')>=0:
            try:
                lineList = line.strip('\n').strip('\r').strip(' ').split(':')[1].split('|')
                retObj['crsfdrTable'] = lineList[0]
                retObj['dateDiff'] = lineList[1].split('=')[1]
            except:
                retObj['msg'] = 'Incorrect Token.txt => LTECRSFDR'
                retObj['ret'] = False
                return retObj
    retObj['ret'] = True       
    return retObj
    

def endProcess(lockpath, exitCode):
   if os.path.isdir(lockpath):
      logMessage("Remove lock %s" % lockpath)
      try:
         removeDir(lockpath)
      except Exception as e:
         logMessage("Remove lock %s failed\n%s" % (lockpath, e))
         exitCode = 1

   sys.exit(exitCode)
    

# get mesos master with mesos-resolve
def getMesosMaster(zkStr='zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos'):
   cmd = "mesos-resolve %s" % zkStr
   ret = subprocessShellExecute(cmd)
   #print ret['cmd']
   #print ret['ret']
   #print ret['retCode']
   #print "-%s-" % ret['outmsg'].strip()
   #print "-%s-" % ret['errmsg'].strip()
   if ret['ret']:
      masterStrArr = ret['outmsg'].strip().split(':')
   else: # error
      masterStrArr = []

   # init
   master = ''
   masterPort = '0'
   if len(masterStrArr) >= 2: # error getting master
      master = masterStrArr[0]
      masterPort = masterStrArr[1]

   return master, int(masterPort)

# get parquet info from folder structure
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

# get parquet info from folder structure
def getInfoFromPQNokia(parquetLocation):

   finalPqList = dict()

   #pqList = glob.glob(parquetLocation+"/*_date=*")
   pqList = glob.glob(parquetLocation+"/*_ft=*")
   if len(pqList) <= 0:  # no file type folder
      return None
   else:
      for filetype in pqList:

         filetypeStr = filetype.split("_ft=")[1]
         finalPqList[filetypeStr] = dict()

         pqDateList = glob.glob(filetype+"/*_date=*")
         if len(pqDateList) <= 0:  # no date folder
            pass
         else:   
            for date in pqDateList:

               dateStr = date.split("_date=")[1]
               finalPqList[filetypeStr][dateStr] = dict()

               pqMarketList = glob.glob(date+"/*_market=*")
               if len(pqMarketList) <= 0:  # no market folder
                  pass
               else:
                  for market in pqMarketList:

                     marketStr = market.split("_market=")[1]
                     finalPqList[filetypeStr][dateStr][marketStr] = dict()

                     pqHrList = glob.glob(market+"/*_hr=*")
                     if len(pqHrList) <= 0:  # no hr folder
                        pass
                     else:
                        for hr in pqHrList:

                           hrStr = hr.split("_hr=")[1]
                           finalPqList[filetypeStr][dateStr][marketStr][hrStr] = hr

   return finalPqList
       
def getAvailablePortRand(startport=4040, endport=4055):

   port = random.randint(startport, endport) # inclusive random integer
   return port

def combineFile(fileList, outputFile): # warning, input files will be removed once finished

   newfile = outputFile + '.tmp'

   for file in sorted(fileList, reverse=True):
      if not os.path.exists(newfile): # copy file
         if os.path.exists(file):
            shutil.copy(file, newfile)
      else: # append
         if os.path.exists(file):
            linenum = 0
            # combine file (append w/ skip first line)
            outfile = open(newfile, 'ab')
            with open(file, 'rb') as infile:
               for line in infile:
                  linenum += 1
                  if linenum > 1: # skip first line
                     outfile.write(line)
               outfile.close()

   for file in sorted(fileList, reverse=True):
      os.system("rm -rf '%s'" % file) # remove old output file
   shutil.move(newfile, outputFile)

