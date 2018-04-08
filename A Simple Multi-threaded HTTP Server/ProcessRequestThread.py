#!/usr/bin/env python
import threading
import mimetypes
import os
import sys

from email.utils import formatdate


class ProcessRequestThread(threading.Thread):
    file_access_log = {}
    lock = threading.Lock()

    def __init__(self, connection, client_address):
        super().__init__()
        self.connection = connection
        self.client_address = client_address
        self.file_name = " "
        self.response_ok = False

    def processing_req(self):
        # while True:
        try:
            data = self.connection.recv(1024)
            if data:
                if not [name for name in os.listdir(os.getcwd()) if os.path.isdir(name) and name == "www"]:
                    # print("Error : File not found")
                    self.connection.sendall(self.create_fail_response())
                else:
                    response = self.create_response(data)
                    self.connection.sendall(response)

        except:
            pass
        finally:
            self.close_connection()
            self.print_output()

    def run(self):
        self.processing_req()

    def create_response(self, data):
        list_lines = data.decode("utf-8").splitlines()

        first_line = list_lines[0].split()
        HTTP_Version = first_line[2]
        self.file_name = ''.join(first_line[1][1:])
        file_path = os.path.join(os.getcwd(), "www", self.file_name)
        if os.path.exists(file_path):
            return self.create_ok_response(HTTP_Version, file_path)
        else:
            return self.create_fail_response()

    @staticmethod
    def create_fail_response():
        file_type = "text/html"
        content = '<!DOCTYPE HTML><html><title>404 Error</title><body>404 Not Found</body></html>'

        size = len(content.encode('utf-8'))

        response = "HTTP/1.1 404 Not Found\r\n\
Date: {}\r\n\
Server: Swaghma1 Server ({})\r\n\
Accept-Ranges: bytes\r\n\
Content-Length: {}\r\n\
Content-Type: {}\r\n\
{}\r\n".format(formatdate(timeval=None, localtime=False, usegmt=True), sys.platform, size, file_type, content)

        return response.encode('utf-8')

    def create_ok_response(self, HTTP_Version, file_path):
        size = os.path.getsize(file_path)
        last_update = formatdate(
            timeval=os.stat(file_path).st_mtime,
            localtime=False,
            usegmt=True)
        file_type = mimetypes.MimeTypes().guess_type(file_path)[0]
        if file_type is None:
            file_type = "application/octet-stream"
        content = ""
        with open(file_path) as f:
            content = f.read()

        response = "{} 200 OK \r\n \
        Date: {} \r\n \
        Server: Swaghma1 Server ({}) \r\n \
        Last-Modified: {} \r\n \
        Accept-Ranges: bytes \r\n \
        Content-Length: {} \r\n \
        Content-Type: {} \r\n \
        {}\r\n\r\n".format(HTTP_Version, formatdate(timeval=None, localtime=False, usegmt=True), sys.platform,
                           last_update,
                           size,
                           file_type, content)

        self.response_ok = True
        return response.encode('utf-8')

    def print_output(self):
        if self.response_ok:
            ProcessRequestThread.lock.acquire()
            if self.file_name in ProcessRequestThread.file_access_log:
                ProcessRequestThread.file_access_log[self.file_name] += 1
            else:
                ProcessRequestThread.file_access_log[self.file_name] = 1
            ProcessRequestThread.lock.release()

            print("{}|{}|{}|{}".format(self.file_name, self.client_address[0], self.client_address[1],
                                     ProcessRequestThread.file_access_log[self.file_name]))

    def close_connection(self):
        self.connection.close()