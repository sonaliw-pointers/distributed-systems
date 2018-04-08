# !/usr/bin/env python
import hashlib
import sys
import glob

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from chord.ttypes import NodeID, SystemException, RFileMetadata, RFile


def main():
    ip = sys.argv[1]
    port = sys.argv[2]

    # Make socket
    transport = TSocket.TSocket(ip, int(port, 10))

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = FileStore.Client(protocol)

    # Connect!
    transport.open()
    #
    # node = client.findSucc('1d7e6801e8f1784f36b8ac4dc996047a8a3975af6f30857193415ad5007aa3ef')
    try:
        meta = RFileMetadata(filename='abc.txt', version=1, owner='sonali',
                             contentHash=decode_sha256('file .....................'))
        rFile = RFile(meta=meta, content='file .....................')

        client.writeFile(rFile)
    except Thrift.TException as tx:
        print('%s' % tx.message)

    try:
        meta = RFileMetadata(filename='abc.txt', version=1, owner='sonali W',
                             contentHash=decode_sha256('file .....................'))
        rFile = RFile(meta=meta, content='file .....................')

        client.writeFile(rFile)
    except Thrift.TException as tx:
        print('%s' % tx.message)

    try:
        meta = RFileMetadata(filename='abc.txt', version=1, owner='sonali P',
                             contentHash=decode_sha256('file .....................'))
        rFile = RFile(meta=meta, content='file .....................')

        client.writeFile(rFile)
    except Thrift.TException as tx:
        print('%s' % tx.message)

    try:
        meta = RFileMetadata(filename='abc.txt', version=1, owner='sonali p W',
                             contentHash=decode_sha256('file .....................'))
        rFile = RFile(meta=meta, content='file .....................')

        client.writeFile(rFile)
    except Thrift.TException as tx:
        print('%s' % tx.message)

    try:
        rfile = client.readFile(filename='abc.txt', owner='sonali')
        print(rfile)
    except Thrift.TException as tx:
        print('%s' % tx.message)

    try:
        client.readFile(filename='abc.txt', owner='sonali W')
        print(rfile)

    except Thrift.TException as tx:
        print('%s' % tx.message)

    try:
        client.readFile(filename='abc.txt', owner='sonali P')
        print(rfile)

    except Thrift.TException as tx:
        print('%s' % tx.message)

    try:
        client.readFile(filename='abc.txt', owner='sonali p W')
        print(rfile)

    except Thrift.TException as tx:
        print('%s' % tx.message)
    #
    # # print (node)

    # Close!
    transport.close()


def decode_sha256(s):
    sha256 = hashlib.sha256()
    sha256.update(s)
    return sha256.hexdigest()


if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
