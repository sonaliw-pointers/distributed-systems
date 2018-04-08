#!/usr/bin/python
import random
import socket
import threading
from math import floor
import time
import sys

import select

import bank_pb2

exit_app = False


class Branch:
    recv_lock = threading.RLock()
    bal_lock = threading.RLock()
    # recv_lock = threading.RLock()
    store_lock = threading.RLock()

    def __init__(self, branch):
        self.branch = branch

        # listener
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind((branch.ip, int(branch.port)))

        print self.listener

        # ================ start code for testing on windows
        # self.listener.bind((socket.gethostname(), int(0)))
        # self.branch.port = int(self.listener.getsockname()[1])
        # self.branch.name = branch.name + str(self.branch.port)
        # ================ end code for testing on windows

        self.listener.listen(100)
        print self.branch.name, " ", self.branch.ip, " ", self.branch.port

        self.all_branches = []
        self.connections = []
        self.branch_connection_dict = {}  # branch_index : conn
        self.connections_branch_dict = {}  # conn : branch

        # local state
        self.balance = 0
        self.channel_states = {}  # branch_index: clannel_state

        # snap shots
        self.snap_shot_list = {}
        self.marker_list = []

    @property
    def balance(self):
        with Branch.bal_lock:
            return self.balance

    @balance.setter
    def balance(self, val):
        with Branch.bal_lock:
            self.balance = val

    def start_recv_forever(self):
        print "start_recv_forever"
        while True:
            try:
                total_conn = [c for c in self.connections if c ]
                total_conn.append(self.listener)
                # print total_conn
                conn_list, _, _ = select.select(total_conn, [], [])
                branch = None
                for conn in conn_list:
                    print conn , self.listener
                    print  "conn: ", conn.getsockname()

                    if conn == self.listener:
                        branch = self.branch
                        print  "calling Accept"
                        # print  "conn: ", conn.getsockname()

                        sender, _ = self.listener.accept()
                        # print "sender" , sender.getsockname()
                        # branch = self.get_branch(sender)
                        # print  "conn: ", conn.getsockname()
                        # self.store_connection(connection=sender, conn_branch=branch)
                        self.start_recv(connection=conn)
                    elif  conn:
                        # print  "conn: ", conn.getsockname()
                        print "accept else"
                        self.store_connection(connection=conn,conn_branch=branch)
                        self.start_recv(connection=conn)
                    else:
                        print " no recieve"
            except:
                continue

    def store_connection(self, connection , conn_branch = None):
        print " started store connection"
        with self.store_lock:
            if connection not in self.connections_branch_dict:
                if not conn_branch:
                    conn_branch = self.get_branch(connection)
                if conn_branch:
                    print conn_branch.name
                    self.connections_branch_dict[connection] = conn_branch
                    conn_branch_i = self.all_branches.index(conn_branch)
                    self.branch_connection_dict[conn_branch_i] = connection
                    self.connections[conn_branch_i] = connection
                    print ":" , self.connections

    def start_recv(self, connection):
        # time.sleep(5/1000)
        with Branch.recv_lock:
            try:
                message = connection.recv(2048)
                if message:
                    print "start_recv", message
                    ''' process with multithreading'''
                    self.process_recv_message(message=message, connection=connection)
            except:
                pass

    def process_recv_message(self, message, connection):
        print "process_recv_message"
        branch_msg = bank_pb2.BranchMessage()
        branch_msg.ParseFromString(message)

        if branch_msg.WhichOneof("branch_message") == "init_branch":  # recv
            self.recv_init_branch(branch_msg.init_branch)
        elif branch_msg.WhichOneof("branch_message") == "transfer":  # recv send
            self.recv_transfer(branch_msg.transfer, connection=connection)
        elif branch_msg.WhichOneof("branch_message") == "init_snapshot":  # recv
            self.recv_init_snapshot(branch_msg.init_snapshot)
        elif branch_msg.WhichOneof("branch_message") == "marker":  # recv send
            self.recv_marker(branch_msg.marker, connection)
        elif branch_msg.WhichOneof("branch_message") == "retrieve_snapshot":  # recv
            self.recv_retrieve_snapshot(branch_msg.retrieve_snapshot, connection)
        else:
            print "recieved ", message

    def recv_init_branch(self, init_branch):
        print "recieved recv_init_branch"
        self.balance = init_branch.balance
        self.all_branches = [b for b in init_branch.all_branches]
        self.channel_states = {b: 0 for b in range(len(init_branch.all_branches))}
        self.connections = [None for b in init_branch.all_branches]
        print 'Initialized branch balance :', self.balance

        self.connect_to_all_branches()
        self.start_random_transfer()

        # f_stop = threading.Event()
        # self.start_random_transfer(f_stop)


    def connect_to_all_branches(self):
        print "connect_to_all_branches "
        i = self.all_branches.index(self.branch)
        print self.all_branches
        print "index of current" , i
        for b in range(i + 1, len(self.all_branches)):
            try:
                each_branch = self.all_branches[b]
                print " branch " , each_branch

                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn.connect((each_branch.ip, each_branch.port))
                print "connect To :", each_branch.name, conn
                self.store_connection(connection=conn, conn_branch = each_branch)
            except:
                continue

    def recv_transfer(self, transfer, connection):
        print " start recieve transfer"
        branch = self.connections_branch_dict[connection]
        branch_index = self.all_branches.index(branch)
        self.channel_states[branch_index] = transfer.money
        if transfer.money > 0:
            val = transfer.money + self.balance
            self.balance = val
        print '/nRecieved branch balance :', transfer.money, "Total: ", self.balance
        pass

    def send_transfer(self, money, conn):
        print " sending transfer of ", money
        val = self.balance - money
        if val > 0:
            branch_msg = bank_pb2.BranchMessage()
            temp = branch_msg.transfer
            temp.money = money
            message = branch_msg.SerializeToString()
            print message

            try:
                conn.sendall(message)
                self.balance = val
            finally:
                print '/nSend branch balance :', money, "Total :", self.balance

    def start_random_transfer(self):
        print "started random transfer"
        amt = self.generate_random_amt()

        for conn in self.connections_branch_dict:
            self.send_transfer(amt, conn)

        # print "amt " , amt
        # random_i = random.randrange(0, len(self.all_branches)-1, 1)
        #
        # if self.all_branches[random_i] == self.branch:
        #     random_i = random.randrange(0, len(self.all_branches) - 1, 1)
        #
        # print "random_i" ,random_i , self.branch_connection_dict
        # conn = self.branch_connection_dict[random_i]
        #
        # print "random conn" ,random_i , conn , self.all_branches[random_i]
        # if conn:
        #     self.send_transfer(amt, conn)
        #
        # if not stop.is_set():
        #     # call f() again in 60 seconds
        #     threading.Timer(6/1000, self.start_random_transfer, [stop]).start()

        print " "



    def generate_random_amt(self):
        amt = floor(random.randrange(1, 5, 1) * self.balance / 100)
        return amt

    def recv_init_snapshot(self, init_snapshot):
        print '/nRecieved InitSnapshot :'
        snap_id = init_snapshot.snapshot_id
        '''record state'''
        local_state = bank_pb2.ReturnSnapshot.LocalSnapshot()
        local_state.snapshot_id = snap_id
        local_state.balance = self.balance
        self.snap_shot_list[snap_id] = local_state

        '''send out markers to all'''
        branch_msg = bank_pb2.BranchMessage()
        branch_msg.marker.snapshot_id = snap_id
        message = branch_msg.SerializeToString()
        self.send_messsage_all(message=message)

    def recv_marker(self, marker, connection):
        print '/nRecieved Marker :'
        snap_id = marker.snapshot_id
        sender = self.connections_branch_dict[connection]
        local_snap = self.snap_shot_list[snap_id]

        if snap_id not in self.marker_list:
            self.marker_list.append(snap_id)

            '''records its own local state (balance)'''
            local_snap.balance = self.balance

            '''records the state of the incoming channel from the sender to itself as empty'''
            channel_state = [self.channel_states[b] if self.all_branches[b] != sender else 0 for b in
                             range(len(self.all_branches))]
            local_snap.channel_state = channel_state

            '''immediately starts recording on other incoming channels'''

            '''sends out Marker messages to all of its outgoing channels'''
            branch_msg = bank_pb2.BranchMessage()
            branch_msg.marker.snapshot_id = snap_id
            message = branch_msg.SerializeToString()
            self.send_messsage_all(message=message)
        else:
            '''records the state of the incoming channel as the sequence of money
                transfers that arrived between 
                when it recorded its local state and 
                when it received the Marker'''
            local_snap = self.snap_shot_list[snap_id]
            channel_state = [self.channel_states[b] for b in range(len(self.all_branches))]
            local_snap.channel_state = channel_state

    def recv_retrieve_snapshot(self, retrieve_snapshot, connection):
        print '/nRecieved RetrieveSnapshot :'
        snap_id = retrieve_snapshot.snapshot_id
        '''its recorded local and channel states and 
        return them to the caller (i.e., the controller) by sending a returnSnapshot'''
        if snap_id in self.snap_shot_list:
            branch_msg = bank_pb2.BranchMessage()
            temp = branch_msg.return_snapshot
            temp.snapshot_id = snap_id
            temp.local_snapshot = self.snap_shot_list[snap_id]
            message = branch_msg.SerializeToString()
            connection.sendall(message)

    def send_message(self, message, branch_index):
        try:
            conn = self.branch_connection_dict[branch_index]
            conn.sendall(message)
        finally:
            pass

    def send_messsage_all(self, message):
        for conn in self.connections_branch_dict:
            conn.sendall(message)

    def server_close(self):
        pass

    def get_branch(self, connection):
        addr = connection.getsockname()
        print addr
        for b in self.all_branches:
            if b.ip == addr[0] and b.port == int(addr[1]):
                return b


def main():
    name = sys.argv[1]
    port = sys.argv[2]

    branch = bank_pb2.InitBranch.Branch()
    branch.name = name
    branch.port = int(port)
    branch.ip = socket.gethostbyname(socket.getfqdn())

    branch_server = Branch(branch=branch)
    branch_server.start_recv_forever()



if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        exit_app = True
        print "interupt...."
        raise
