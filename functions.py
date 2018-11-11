import time
import json
import uuid
import shutil
import os
import ast
import logging
from logging import StreamHandler

def evaldict(inputDict):
    """Output from the server needs to be evaluated"""
    out = {}
    try:
        out = ast.literal_eval(str(inputDict).encode('utf-8'))
    except ValueError as ex:
        raise ValueError("Failed to literal eval dict. Err:%s " % ex)
    except SyntaxError as ex:
        raise SyntaxError("Failed to literal eval dict. Err:%s " % ex)
    return out

def createDirs(fullDirPath):
    """ Create Directories on fullDirPath"""
    if not os.path.isdir(fullDirPath):
        try:
            os.makedirs(fullDirPath)
        except OSError as ex:
            print 'Received exception creating %s directory. Exception: %s' % (fullDirPath, ex)
    return

def getStreamLogger(logLevel='DEBUG'):
    """ Get Stream Logger """
    levels = {'FATAL': logging.FATAL,
              'ERROR': logging.ERROR,
              'WARNING': logging.WARNING,
              'INFO': logging.INFO,
              'DEBUG': logging.DEBUG}
    logger = logging.getLogger()
    handler = StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                                  datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(levels[logLevel])
    return logger

def getFileContentAsJson(inputFile):
    """ Get file content as json """
    out = {}
    if os.path.isfile(inputFile):
        with open(inputFile, 'r') as fd:
            try:
                out = json.load(fd)
            except ValueError:
                print fd.seek(0)
                out = evaldict(fd.read())
    return out

def getAllFileContent(inputFile):
    """ Get all file content as a string """
    if os.path.isfile(inputFile):
        with open(inputFile, 'r') as fd:
            return fd.read()
    raise Exception('File %s was not found on the system.' % inputFile)

class contentDB(object):
    """ File Saver, loader class """
    def __init__(self, logger=None):
        self.locked = False
        self.logger = logger
        self.retryTime = 3600
        self.sleepTimer = 0.1

    def resetConfig(self):
        """Reset Configuration """
        self.retryTime = 3600

    def getFileContentAsJson(self, inputFile):
        """ Get file content as json """
        return getFileContentAsJson(inputFile)

    def getHash(self, inputText):
        newuuid4 = str(uuid.uuid4())
        return str(newuuid4 + inputText)

    def getLockStat(self, outFile, inHash):
        lockFile = outFile + '.lock'
        lockKey = ''
        if os.path.isfile(lockFile):
            with open(lockFile, 'r') as fd:
                lockKey = fd.read().replace('\n', '')
        else:
            with open(lockFile, 'w') as fd:
                fd.write(inHash)
            if os.path.isfile(lockFile):
                with open(lockFile, 'r') as fd:
                    lockKey = fd.read().replace('\n', '')
        if lockKey == inHash:
            return True
        return False

    def dumpFileContentAsJson(self, outFile, content, newHash=None):
        """ Dump File content with locks """
        lockName = outFile + '.lock'
        if not newHash:
            newHash = self.getHash("This-to-replace-with-date-and-Service-Name-Direct-only-DTN")
        try:
            if self.getLockStat(outFile, newHash):
                self.locked = True
                tmpoutFile = outFile + '.tmp'
                with open(tmpoutFile, 'w+') as fd:
                    json.dump(content, fd)
                shutil.move(tmpoutFile, outFile)
                self.locked = False
                self.removeFile(lockName)
                return True
            else:
                time.sleep(self.sleepTimer)
            return False  # Meaning it failed to update and forcing to come back later
        finally:
            if self.locked:
                self.logger.info('I am still locked. How come? My Hash %s', newHash)
                # Remove Lock file!
                lockName = outFile + '.lock'
                self.removeFile(lockName)
                self.locked = False
        return True

    def saveContent(self, destFileName, outputDict):
        """ Saves all content to a file """
        self.resetConfig()
        newHash = self.getHash("This-to-replace-with-date-and-Service-Name")
        success, retry = False, True
        while retry:
            success = self.dumpFileContentAsJson(destFileName, outputDict, newHash)
            if not success:
                time.sleep(self.sleepTimer)
                self.retryTime -= 1
                if self.retryTime <= 0:
                    return success
            else:
                return success
        return success

    def removeFile(self, fileLoc):
        """ Remove file """
        if os.path.isfile(fileLoc):
            os.unlink(fileLoc)
            return True
        return False
