# !/usr/bin/env python
import glob
from sys import platform
import sys
if platform == "linux" or platform == "linux2":
    sys.path.append('./gen-py')
    sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])


import hashlib
import socket
import threading
import time

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from DistributedStore import DistributedStore
from DistributedStore.ttypes import NodeID, SystemException, MetaData, Data

import logging
logging.basicConfig(level=logging.DEBUG)


class Client:
    def __init__(self,nodeId):
        self.cordinatorId = nodeId
        self.cordinator, self.transport = self.connectToNode(self.cordinatorId)


    def connectToNode(self,node):
        # Make socket
        transport = TSocket.TSocket(node.ip, int(node.port))
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = DistributedStore.Client(protocol)
        try:
            transport.open()
        except:
            client = None

        return client , transport

    def closeTransport(self):
        self.transport.close()

    def processWrite(self,query):
        consistency = query[0]
        key = int(query[1])
        value = query[2]

        print("Sending Data : ", key)
        cord, trans = self.connectToNode(self.cordinatorId)
        try:
            # print(self.cordinator.getNodeSucc())
            cord.writeToAllReplicas(key=key,value=value , consistency=int(consistency))
            print('Write successful')
        except Thrift.TException as tx:
            print('%s' % tx.message)
        trans.close()


    def processRead(self,query):
        consistency = query[0]
        key = int(query[1])
        value = ""

        print("Sending Read Key : ", key)
        metaData = MetaData(owner=self.cordinatorId, timeStamp=str(time.time()))
        data = Data(key=key, value=str(value), metaData=metaData)
        cord, trans = self.connectToNode(self.cordinatorId)

        try:
            value = cord.readToAllReplicas(data=data, consistency=int(consistency))
            print('Read successful ', key, '-->', value)
        except Thrift.TException as tx:
            print('%s' % tx.message)
        trans.close()



def main():
    ip = sys.argv[1]
    port = sys.argv[2]
    node = NodeID(id=decode_sha256(ip + ":" + str(port)), ip=ip, port=int(port, 10))
    client = Client(nodeId=node)
    while 1:
        try:
            req = input('Enter request [GET/PUT] | [C] | [Key] | [value/?]')
            req = req.strip().split(sep='|')
            if len(req) != 4:
                print('Wrong request Format')
            else:
                if req[0].lower().strip() == 'get':
                    client.processRead(req[1:])
                elif req[0].lower().strip() == 'put':
                    client.processWrite(req[1:])
        except Thrift.TException as tx:
            print('%s' % tx.message)
        finally:
            pass

def decode_sha256(s):
    sha256 = hashlib.sha256()
    sha256.update(s.encode())
    return sha256.hexdigest()

if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
