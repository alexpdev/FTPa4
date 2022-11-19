import sys
import os
import socket
import time
import struct
import logging

logger = logging.getLogger(__name__)
fh = logging.FileHandler('./srvdbg.log')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
BUFF_SIZE = 1024

class FTPServer:

    def __init__(self,host=None,port=None):
        if not host:
            host = socket.gethostname()
        self.host = host
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.port = port
        self.addr = (socket.gethostbyname(host), port)
        self.sock.bind(self.addr)
        self.sock.listen(1)
        self.conn, self.address = self.sock.accept()

    def _upld(self):

        self.conn.send("1")
        file_name_size = struct.unpack("h", self.conn.recv(2))[0]
        file_name = self.conn.recv(file_name_size)
        self.conn.send("1")
        file_size = struct.unpack("i", self.conn.recv(4))[0]
        start_time = time.time()
        with open(file_name, "wb") as outfile:
            received = 0
            while received < file_size:
                content = self.conn.recv(BUFF_SIZE)
                outfile.write(content)
                received += BUFF_SIZE
        self.conn.send(struct.pack("f", time.time() - start_time))
        self.conn.send(struct.pack("i", file_size))

    def _list(self):
        filelist = os.listdir(os.getcwd())
        self.conn.send(struct.pack("i", len(filelist)))
        all_dir_size = 0
        for filename in filelist:
            self.conn.send(struct.pack("i", sys.getsizeof(filename)))
            self.conn.send(filename)
            filesize = os.path.getsize(filename)
            self.conn.send(struct.pack("i", filesize))
            all_dir_size += filesize
            self.conn.recv(BUFF_SIZE)
        self.conn.send(struct.pack("i", all_dir_size))
        self.conn.recv(BUFF_SIZE)

    def _dwld(self):
        self.conn.send("1")
        filename_len = struct.unpack("h", self.conn.recv(2))[0]
        filename = self.conn.recv(filename_len)
        if os.path.isfile(filename):
            self.conn.send(struct.pack("i", os.path.getsize(filename)))
        else:
            self.conn.send(struct.pack("i", -1))
        self.conn.recv(BUFF_SIZE)
        start = time.time()
        with open(filename, "rb") as binfile:
            content = bytearray(BUFF_SIZE)
            while True:
                size = binfile.readinto(content)
                if size < BUFF_SIZE:
                    self.conn.send(content[:size])
                    break
                self.conn.send(content)
        self.conn.recv(BUFF_SIZE)
        self.conn.send(struct.pack("f", time.time() - start))


    def _delf(self):
        self.conn.send("1")
        filenamelen = struct.unpack("h", self.conn.recv(2))[0]
        filename = self.conn.recv(filenamelen)
        if os.path.isfile(filename):
            self.conn.send(struct.pack("i", 1))
        else:
            self.conn.send(struct.pack("i", -1))
        confirm_delete = self.conn.recv(BUFF_SIZE)
        if confirm_delete == "Y":
            try:
                os.remove(filename)
                self.conn.send(struct.pack("i", 1))
            except:
                self.conn.send(struct.pack("i", -1))

    def quit(self):
        self.conn.send("1")
        self.conn.close()
        self.sock.close()
        os.execl(sys.executable, sys.executable, *sys.argv)

    def run(self):
        commands = {
            "UPLD": self._upld,
            "LIST": self._list,
            "DWLD": self._dwld,
            "DELF": self._delf,
            "QUIT": self._quit
        }
        while True:
            data = self.conn.recv(BUFF_SIZE)
            if data in commands:
                commands.get(data)()
            data = None
