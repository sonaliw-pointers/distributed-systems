#!/usr/bin/python
import random
import socket
import sys
import threading

import time

import select

import bank_pb2

exit_app = False


class Controller:
    recv_lock = threading.RLock()

    def __init__(self):
        self.init_balance = 0
        self.snap_shot_id = 1
        self.snap_results = {}

        self.all_branches = []
        self.connections = []
        self.branch_conn_dict = {} # brach_index: conn
        self.conn_branch_dict = {} # connection : branch
        self.snap_results_counts = {} # id : count

    def init_branch(self, balance, file):
        self.init_balance = balance
        for l in open(file):
            self.create_socket(l[:-1])

        balance_per_branch = balance / len(self.all_branches)

        for b in range(len(self.all_branches)):
            self.init_each_server(b, balance_per_branch)

        recv_thread = threading.Thread(target=self.start_recv_forever)
        recv_thread.daemon = True
        recv_thread.start()

    def create_socket(self, line):
        data = line.strip().split()

        branch = bank_pb2.InitBranch.Branch()
        branch.name = data[0]
        branch.ip = data[1]
        branch.port = int(data[2])

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            sock.connect((branch.ip, branch.port))
        finally:
            self.all_branches.append(branch)
            self.conn_branch_dict[sock] = branch
            self.branch_conn_dict[len(self.all_branches)-1] = sock
            print branch

        print sock.getsockname()

    def init_each_server(self, branch_index, init_balance):
        branch = self.all_branches[branch_index]
        branch_msg = bank_pb2.BranchMessage()
        init_branch_temp = branch_msg.init_branch
        init_branch_temp.balance = init_balance
        # init_branch_temp.all_branches = self.all_branches

        for b in self.all_branches:
            temp_branch = init_branch_temp.all_branches.add()
            temp_branch.name = b.name
            temp_branch.ip = b.ip
            temp_branch.port = b.port

        message = branch_msg.SerializeToString()

        print self.branch_conn_dict

        print "initBranch message :" , branch.name
        print message

        try:
            self.branch_conn_dict[branch_index].sendall(message)
        finally:
            # server.close()
            pass

    def start_recv_forever(self):
        while not exit_app:
            try:
                conn_list, _, _ = select.select(self.connections,[],[])
                for conn in conn_list:
                    print " recieved" , conn.getsockname()
                    self.receive(connection=conn)
            except:
                continue

    def receive(self, connection):
        with Controller.recv_lock:
            msg = connection.recv(2048)
            branch_msg = bank_pb2.BranchMessage()
            branch_msg.ParseFromString(msg)
            if branch_msg.WhichOneof("branch_message") == "return_snapshot":
                self.recv_return_snapshot(branch_msg.return_snapshot,connection=connection)
            else:
                print "recieved : ", msg

    def send_init_snapshot(self):
        branch_msg = bank_pb2.BranchMessage()
        temp = branch_msg.init_snapshot
        temp.snapshot_id = self.snap_shot_id
        self.snap_results[self.snap_shot_id] = []
        self.snap_results_counts[self.snap_shot_id] = 0
        message = branch_msg.SerializeToString()
        try:
            print "send_init_snapshot" , message
            rand_index = random.randrange(0, len(self.all_branches), 1)
            rand_conn = self.branch_conn_dict[rand_index]
            rand_conn.sendall(message)
        finally:
            self.snap_shot_id += 1

    def take_snapshot(self):
        while not exit_app:
            self.send_init_snapshot()
            self.send_retrieve_snapshot()
            # self.print_snapshot(self.snap_shot_id)
            pass


    def send_retrieve_snapshot(self):
        time.sleep(5)
        self.snap_results_counts[self.snap_shot_id] = 0
        # snap_id = random.randrange(1, self.snap_shot_id, 1)
        snap_id = self.snap_shot_id
        branch_msg = bank_pb2.BranchMessage()
        temp = branch_msg.retrieve_snapshot
        temp.snapshot_id = snap_id
        message = branch_msg.SerializeToString()
        print message
        for c in self.conn_branch_dict:
            print c.getsockname()
            c.sendall(message)

    def recv_return_snapshot(self, return_snapshot, connection):
        sender = self.conn_branch_dict[connection]
        id = return_snapshot.snapshot_id
        self.snap_results[id].append((sender,return_snapshot.local_snapshot))
        self.snap_results_counts[id] += 1

        if  len(self.all_branches) == self.snap_results_counts[id]:
            self.print_snapshot(id)


    def print_snapshot(self, id):
        if id in self.snap_results:
            sender = self.snap_results[id][0]
            local_snap = self.snap_results[id][1]

            message = "%s : %s" % (sender.name, local_snap.balance)
            channels = []
            for b in range(len(self.all_branches)):
                branch = self.all_branches[b]
                channels.append("%s->%s: %s" % (branch.name, sender.name, local_snap.channel_state[b]))

            message = message + ' '.join(channels)

            print "snapshot_id: ", id
            print message



def main():
    branch_balance = sys.argv[1]
    branches_file = sys.argv[2]

    controllerServer = Controller()

    # send messages initbranch
    controllerServer.init_branch(int(branch_balance), branches_file)
    controllerServer.take_snapshot()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        exit_app = True
        raise
