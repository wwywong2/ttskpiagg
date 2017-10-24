#!/usr/bin/python
import os, glob, uuid
import sys, subprocess, json
import socket
import time # This is required to include time module

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

def logMessage(message, logFilePt=None):

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
    
    
       
       
