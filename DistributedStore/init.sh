#!/bin/bash +vx
export PATH=$PATH:/home/yaoliu/src_code/local/bin
thrift --gen py DistributedStore.thrift
python initReplica.py $1