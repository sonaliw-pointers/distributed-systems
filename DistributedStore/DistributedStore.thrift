typedef string UserID

exception SystemException {
  1: optional string message
}
struct NodeID {
  1: string id;
  2: string ip;
  3: i32 port;
}
struct MetaData{
1: NodeID owner;
2: string timeStamp;
}
struct Data{
1: i32 key;
2: string value;
3: MetaData metaData
}
struct MsgWrite{
1: string transactionId,
2: NodeID server;
3: string status;
4: i32 key;
}
struct MsgRead{
1: string transactionId,
2: NodeID server;
3: string status;
4: Data data
}
struct MsgWriteRepair{
1: string transactionId,
2: NodeID server;
3: string status;
}


service DistributedStore {
  void write(1: Data data,2: string transactionId)
       throws (1: SystemException systemException),

  void writeToAllReplicas(1: i32 key,2:string value, 3:i32 consistency)
      throws (1: SystemException systemException),

  void setWriteStatus(1: string transactionId,2: MsgWrite msg)
       throws (1: SystemException systemException),

  void read(1: Data data,2: string transactionId)
        throws (1: SystemException systemException),

  string readToAllReplicas(1: Data data, 2:i32 consistency)
      throws (1: SystemException systemException),

  void setReadStatus(1: string transactionId,2: MsgRead msg)
       throws (1: SystemException systemException),

  void setReplicationtable(1: list<NodeID> node_list)
       throws (1: SystemException systemException),

  list<NodeID>  getReplicas()
       throws (1: SystemException systemException),

  void writeRepair(1: list<Data>dataHints , 2: string transId,3:NodeID coordinator)
      throws (1: SystemException systemException),

  void setWriteRepair(1: string transId, 2:MsgWriteRepair msg)
      throws (1: SystemException systemException),

  void readRepair(1: Data data)
      throws (1: SystemException systemException),

}
