#!/usr/bin/env python


import socket
from ProcessRequestThread import ProcessRequestThread


def processs_client_request(serversocket):
    threads = []
    connection_count = 0
    while True:
        # Wait for a connection
        # print('waiting for a connection')
        connection, client_address = serversocket.accept()
        try:
            if connection is not None:
                connection_count = connection_count + 1

            req_thread = ProcessRequestThread(connection, client_address)
            threads.append(req_thread)
            req_thread.start()
            # req_thread.join()
        finally:
            # Clean up the connection
            if connection_count == 5:
                break

    # Wait for all threads to complete
    for t in threads:
        t.join()


def main():
    # create an INET, STREAMing socket
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # bind the socket to a public host, and a well-known port
    serversocket.bind((socket.gethostname(), 0))

    print("Host Name : {} \nPort : {} ".format(socket.getfqdn(), serversocket.getsockname()[1]))
    print("{}:{} ".format(socket.getfqdn(), serversocket.getsockname()[1]))

    # become a server socket
    serversocket.listen(5)

    processs_client_request(serversocket)


if __name__ == "__main__":
    main()
