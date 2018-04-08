# !/usr/bin/env python
import glob
import sys

sys.path.append('./gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from chord import FileStore
from chord.ttypes import NodeID, SystemException, RFileMetadata, RFile

import logging

logging.basicConfig(level=logging.DEBUG)


class FileStoreHandler:
    CHORD_MAX_SHA = 2 ** 256 - 1
    CHORD_MIN_SHA = 0

    def __init__(self, nodeId):
        self.fingerTable = None
        self.nodeId = nodeId
        self.rFiles = {}

        print (self.nodeId)

    def writeFile(self, rFile):
        """
        Parameters:
         - rFile
        """
        # print ("\n\nwriteFile({})".format(rFile))
        fileSHA = decode_sha256(rFile.meta.owner + ":" + rFile.meta.filename)
        fileServer = self.findSucc(key=fileSHA)
        # print ("fileServer = {} ".format(fileServer.port))

        if self.nodeId == fileServer:
            self.rFilesLock.acquire()
            print ("Writing to this server....")
            if rFile.meta.filename in self.rFiles:
                existingFile = self.rFiles[rFile.meta.filename]
                existingSHA = decode_sha256(existingFile.meta.owner + ":" + existingFile.meta.filename)
                if fileSHA == existingSHA:
                    # Edit
                    existingFile.content = rFile.content
                    existingFile.meta.version = existingFile.meta.version + 1
                    existingFile.meta.contentHash = decode_sha256(rFile.content)
                else:
                    self.rFilesLock.release()
                    raise SystemException(message='Write Failed: File name already exists ')
            else:
                # create
                meta = RFileMetadata(filename=rFile.meta.filename, version=0, owner=rFile.meta.owner,
                                     contentHash=decode_sha256(rFile.content))
                self.rFiles[rFile.meta.filename] = RFile(meta=meta, content=rFile.content)

            self.rFilesLock.release()
        else:
            raise SystemException(message='Write Failed: Server does not own the file')

        # print('writeFile(%s) completed' % self.rFiles)

    def readFile(self, filename, owner):
        """
        Parameters:
         - filename
         - owner
        """
        print("\n\nreadFile({},{})".format(filename, owner))
        fileToRead = None
        fileSHA = decode_sha256(owner + ":" + filename)
        fileServer = self.findSucc(key=fileSHA)
        # print ("fileServer = {} ".format(fileServer.port))
        if self.nodeId == fileServer:
            if filename in self.rFiles:
                self.rFilesLock.acquire()
                existingFile = self.rFiles[filename]
                existingSHA = decode_sha256(existingFile.meta.owner + ":" + existingFile.meta.filename)
                if fileSHA == existingSHA:
                    fileToRead = existingFile
                    self.rFilesLock.release()
                else:
                    self.rFilesLock.release()
                    raise SystemException(message='Read Failed: Permission denied.....')
            else:
                # create
                raise SystemException(message='Read Failed: File not found')
        else:
            raise SystemException(message='Read Failed : Server does not own the file')

        # print("Completed readFile({},{})".format(filename, owner))
        return fileToRead

    def setFingertable(self, node_list):
        """
        Parameters:
         - node_list
        """
        self.fingerTableLock.acquire()
        self.fingerTable = node_list
        self.fingerTableLock.release()
        # print (node_list)

    def findSucc(self, key):
        """
        Parameters:
         - key
        """
        precNodeId = self.findPred(key=key)

        if self.nodeId == precNodeId:
            succNode = self.getNodeSucc()
        else:
            # RPC call
            client, transport = self.getClientToCallRPC(precNodeId)
            transport.open()
            try:
                succNode = client.getNodeSucc()
            finally:
                # Close!
                transport.close()

        return succNode

    def findPred(self, key):
        """
        Parameters:
         - key
        """
        predNode = None
        item = int( key, 16)
        itemRange = (int(self.nodeId.id, 16), int(self.getNodeSucc().id, 16))

        if self.doesBelongsToHigh(item=item, inputRange=itemRange):
            predNode = self.nodeId
        else:
            closestPred = self.closestPrecedingFinger(key=key)
            if closestPred:
                # RPC call
                client, transport = self.getClientToCallRPC(closestPred)
                transport.open()
                try:
                    predNode = client.findPred(key=key)
                except Exception as tx:
                    print('Failed RPC to %s findPred : %s' % (closestPred.port, tx.message))
                finally:
                    # Close!
                    transport.close()
        return predNode

    def getNodeSucc(self):
        succ = self.fingerTable[0]
        return succ

    def closestPrecedingFinger(self, key):
        for node in reversed(self.fingerTable[1:]):
            item = int('0x' + node.id, 16)
            itemRange = (int('0x' + self.nodeId.id, 16), int('0x' + key, 16))
            if self.doesBelongsToExclude(item=item, inputRange=itemRange):
                return node

    @staticmethod
    def doesBelongsToExclude(item, inputRange):
        belong = False
        if inputRange[0] < inputRange[1]:
            if inputRange[0] < item < inputRange[1]:
                belong = True
        else:
            if inputRange[0] < item <= FileStoreHandler.CHORD_MAX_SHA or \
                                    FileStoreHandler.CHORD_MIN_SHA <= item < inputRange[1]:
                belong = True

        return belong

    @staticmethod
    def doesBelongsToHigh(item, inputRange):
        belong = False
        if inputRange[0] <= inputRange[1]:
            if inputRange[0] < item <= inputRange[1]:
                belong = True
        else:
            if inputRange[0] < item <= FileStoreHandler.CHORD_MAX_SHA or \
                                    FileStoreHandler.CHORD_MIN_SHA <= item <= inputRange[1]:
                belong = True
        return belong

    @staticmethod
    def getClientToCallRPC(nodeId):
        # print ("RPC to {}".format(nodeId.port))
        transport = TSocket.TSocket(nodeId.ip, nodeId.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = FileStore.Client(protocol)
        return client, transport


def decode_sha256(s):
    import hashlib
    sha256 = hashlib.sha256()
    sha256.update(s)
    return sha256.hexdigest()


if __name__ == '__main__':
    port = sys.argv[1]

    import socket

    ip = socket.gethostbyname(socket.getfqdn())
    handler = FileStoreHandler(NodeID(id=decode_sha256(ip + ":" + port), ip=ip, port=int(port, 10)))
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    # # You could do one of these for a multithreaded server
    # server = TServer.TThreadedServer(
    #     processor, transport, tfactory, pfactory)
    # server = TServer.TThreadPoolServer(
    #     processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')
