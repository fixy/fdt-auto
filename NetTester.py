#!/usr/bin/env python
"""
Title                   : Auto FDT TransferTool
Author                  : Justas Balcas
Email                   : justas.balcas (at) cern.ch
@Copyright              : Copyright (C) 2016 California Institute of Technology
Date                    : 2018/11/08
"""
import os
import sys
import json
import copy
import time
import glob
import shlex
import pprint
import urllib2
import subprocess
import psutil
from functions import contentDB, getFileContentAsJson, createDirs, getStreamLogger


def externalCommand(command, communicate=True):
    """Execute External Commands and return stdout and stderr"""
    command = shlex.split(str(command))
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if communicate:
        return proc.communicate()
    return proc

GIST = 'https://gist.githubusercontent.com/juztas/db3a7e6987c99a9bf17a1e74e5461a38/raw/SENSE-DATANODES'

def executeCmd(command, logger):
    """ Execute interfaces commands. """
    logger.info('Asked to execute %s command' % command)
    cmdOut = externalCommand(command, False)
    out, err = cmdOut.communicate()
    msg = 'Command: %s, Out: %s, Err: %s, ReturnCode: %s' % (command, out.rstrip(), err.rstrip(), cmdOut.returncode)
    logger.info(command)
    return out.rstrip(), err.rstrip(), cmdOut.returncode


def getGistContent():
    req = urllib2.Request(GIST)
    try:
        resp = urllib2.urlopen(req)
        return resp.read()
    except urllib2.URLError as e:
        print e.reason
        return ""

def cleanLogs(files):
    for fileName in files:
        if os.path.isfile(fileName):
            os.unlink(fileName)
        with open(fileName, 'w+') as fd:
            fd.write("Time to start %s" % int(time.time()))

class FDTWorker(object):
    def __init__(self, loggerIn):
        self.logger = loggerIn
        self.logDir = "NetTester/logs/"
        createDirs(self.logDir)
        self.fdtLoc = 'fdt.jar'
        self.serverCmd = "java -jar %s -p %%s -P %%s -noupdates" % self.fdtLoc
        self.clientCmd = "java -jar %s -p %%s -P %%s -noupdates -c %%s -nettest" % self.fdtLoc

    def startServer(self, vlandelta, streams=8, orch=True):
        port = vlandelta['vlan']
        cmd = self.serverCmd % (port, streams)
        logFile = "%s/%s-server.json" % (self.logDir, port)
        cleanLogs([logFile, "%s.stdout" % logFile, "%s.stderr" % logFile])
        with open(logFile, 'w+') as fd:
            fd.write('Start server from python')
            fd.write('Command: %s' % cmd)
        self.logger.info('Executing this command %s' % cmd)
        proc = subprocess.Popen(cmd, shell=True, stdout=file("%s.stdout" % logFile, "ab+"), stderr=file("%s.stderr" % logFile, "ab+"))
        self.logger.info("PID: %s", proc.pid)
        return proc.pid

    def startClient(self, vlandelta, streams=8, orch=True):
        ip = None
        cmd = None
        logFile = None
        if orch:
            for item in vlandelta['proc_ips']:
                if item != vlandelta['ip']:
                    ip = item
            port = vlandelta['vlan']
            cmd = self.clientCmd % (port, streams, ip[:-3])
            logFile = "%s/%s-client-%s.json" % (self.logDir, port, ip[:-3])
        else:
            ip = vlandelta['ip']
            port = vlandelta['vlan']
            cmd = self.clientCmd % (port, streams, ip)
            logFile = "%s/%s-client-%s.json" % (self.logDir, port, ip)
        cleanLogs([logFile, "%s.stdout" % logFile, "%s.stderr" % logFile])
        with open(logFile, 'w+') as fd:
            fd.write('Start client from python')
            fd.write('Command: %s' % cmd)
        self.logger.info('Executing this command %s' % cmd)
        proc = subprocess.Popen(cmd, shell=True, stdout=file("%s.stdout" % logFile, "ab+"), stderr=file("%s.stderr" % logFile, "ab+"))
        self.logger.info("PID: %s", proc.pid)
        return proc.pid

    def status(self, spid):
        proc = psutil.Process(spid)
        if proc.status() == psutil.STATUS_ZOMBIE:
            self.stop(spid)
            raise psutil.NoSuchProcess('Pid %s is ZOMBIE Process' % spid)
        return True

    def stop(self, spid):
        proc = psutil.Process(spid)
        proc.terminate()
        return True


class NetTester(object):
    def __init__(self, loggerIn, args=None):
        self.logger = loggerIn
        self.workDir = "NetTester/jsons/"
        self.fdtworker = FDTWorker(self.logger)
        self.customInput = args
        createDirs(self.workDir)
        self.IPs = []
        self.logger.info("==== NetTester Start Work.")
        self.agentdb = contentDB(logger=self.logger)
        self.mypubIP = args

    def stopService(self, pubPID):
        try:
            self.fdtworker.status(pubPID)
            self.fdtworker.stop(pubPID)
        except psutil.NoSuchProcess as ex:
            self.logger.debug(str(ex))
        return

    def publicTransfers(self):
        gitContent = getGistContent()
        publicTrack = self.agentdb.getFileContentAsJson('%s/publictransfer.dict' % self.workDir)
        if not publicTrack:
            publicTrack = {'servers': {}, 'clients': {}}
        mypubIP = self.mypubIP
        servers = []
        for line in gitContent.split('\n'):
            if line.startswith('#'):
                continue
            out = filter(None, line.split(' '))
            if out:
                servers.append(out)
        self.logger.info('I received git content and there is %s servers' % len(servers))
        self.logger.info('My IP is: %s' % mypubIP)
        # Let's check first if my IP is defined at all
        lineNum = -1
        startServer = 0
        startClient = 0
        for iCount in range(0, len(servers)):
            if mypubIP == servers[iCount][1]:
                lineNum = iCount
        if lineNum == -1:
            self.logger.info('My service is not defined in the output of gist. Will not start any transfers')
            self.logger.info('More details: %s' % servers)
            # In case we have any pending transfers, we need to stop them.
            dcopy = copy.deepcopy(publicTrack)
            for pubPort, pubPID in dcopy['servers'].items():
                self.logger.info('Checking status for server %s and %s' % (pubPort, pubPID))
                self.stopService(pubPID)
                del publicTrack['servers'][pubPort]
            for pubPort, pubPID in dcopy['clients'].items():
                self.logger.info('Checking status for client %s and %s' % (pubPort, pubPID))
                self.stopService(pubPID)
                del publicTrack['clients'][pubPort]
        # First we check servers information based on port:
        if lineNum != -1 and servers[lineNum][2]:
            startServer = servers[lineNum][2]
            self.logger.info('My service information %s' % servers[lineNum])
            self.logger.info('Checking public servers if they are up...')
            streams = servers[lineNum][3]
            for portNum in range(0, len(servers)):
                if portNum == lineNum:
                    continue
                if not startServer:
                    continue
                port = servers[lineNum][portNum+5]
                if port in publicTrack['servers'].keys():
                    # Check Status of specific server port
                    try:
                        self.logger.info('Checking status for %s and %s' % (port, publicTrack['servers'][port]))
                        self.fdtworker.status(publicTrack['servers'][port])
                        continue
                    except psutil.NoSuchProcess as ex:
                        self.logger.debug(str(ex))
                # Here means either process is not running or it was never started...
                self.logger.info('Starting server for %s port' % port)
                newpid = self.fdtworker.startServer({'vlan': port}, streams, orch=False) 
                publicTrack['servers'][port] = newpid
        else:
            self.logger.info('This service is not configured to act as Server. Will not start FDT Services')
        # Let's Check all the clients who are pushing data...
        if lineNum != -1:
            startClient = servers[lineNum][3]
        for iCount in range(0, len(servers)):
            if not startClient:
                continue  # We are not starting clients if it is not configured in gist;
            if lineNum == iCount:
                continue  # We just ignore client to do transfers to ourselves.
            if servers[iCount][2]:
                # Means there should be a service listening...
                ip = servers[iCount][1]
                port = servers[iCount][lineNum+5]
                streams = servers[iCount][4]
                # It is a simple matrix, where the column belongs to a specific endhost.
                if ip in publicTrack['clients'].keys():
                    # Check status of specific client port
                    try:
                        self.logger.info('Checking status for %s and transfer to: %s' % (port, ip))
                        self.fdtworker.status(publicTrack['clients'][ip])
                        continue
                    except psutil.NoSuchProcess as ex:
                        self.logger.debug(str(ex))
                self.logger.info('Starting client for %s port to %s' % (port, ip))
                clientpid = self.fdtworker.startClient({'vlan': port, 'ip': ip}, streams, False)
                publicTrack['clients'][ip] = clientpid
        self.agentdb.dumpFileContentAsJson('%s/publictransfer.dict' % self.workDir, publicTrack)

    def start(self):
        self.publicTransfers()

def execute(loggerIn, args):
    ruler = NetTester(loggerIn, args)
    ruler.start()


if __name__ == "__main__":
    execute(loggerIn=getStreamLogger(), args=sys.argv[1])
