#! /usr/bin/python3
# -*- coding: utf-8 -*-

import os
import stat
import logging
import random
import time
import paramiko
import dotenv

logging.basicConfig(level=logging.WARNING)

dotenv.load_dotenv()
host = os.environ["HN"]
port = int(os.environ["PT"])
LOCAL = "/data/torrents/remote"
REMOTE = "/downloads/completed"


class FastTransport(paramiko.Transport):

    def __init__(self, sock):
        super().__init__(sock)
        self.window_size = 2147483647
        self.packetizer.REKEY_BYTES = pow(2, 40)
        self.packetizer.REKEY_PACKETS = pow(2, 40)
        self.hostname, self.port = sock
        self.username = os.environ["UN"]
        self.password = os.environ["PW"]

    def open_connection(self):
        self.connect(username=self.username, password=self.password)

class Connection:

    def __init__(self, host, port):
        self.transport = FastTransport((host, port))
        self.transport.open_connection()
        self.client  = paramiko.SFTPClient.from_transport(self.transport)
        self.local = LOCAL
        self.remote = REMOTE
        self.conn = None

    def __enter__(self):
        if self.conn is not None:
            self.conn.close()
            del self.conn
        self.conn = paramiko.SFTPClient.from_transport(self.transport)
        return self.conn

    def __exit__(self, *args):
        if self.conn is not None:
            self.conn.close()
            del self.conn
        self.conn = None

    def listdir(self, path="."):
        return self.client.listdir(path)

    def chdir(self, path):
        return self.client.chdir(path)

    def getcwd(self):
        return self.client.getcwd()

    def get_size(self, path):
        st = self.client.stat(path)
        return st.st_size

    def open(self, filename, mode='r', bufsize=-1):
        return self.client.open(filename, mode, bufsize)

    def exists(self, path):
        try:
            self.client.stat(path)
        except IOError:
            return False
        return True

    def isdir(self, path):
        try:
            result = stat.S_ISDIR(self.client.stat(path).st_mode)
        except IOError:
            result = False
        return result

    def isfile(self, path):
        try:
            result = stat.S_ISREG(self.client.stat(path).st_mode)
        except IOError:
            result = False
        return result

    def get(self, remote, local, conn):
        if os.path.exists(local):
            raise Exception
        conn.get(remote, local)
        os.chown(local,uid=1000,gid=1000)
        return True

    def _traverse(self, local, remote):
        if self.isfile(remote):
            with self as conn:
                if not os.path.exists(local):
                    then = time.time()
                    conn.get(remote, local)
                    size = os.path.getsize(local)
                    metrics_output(then, size, local)
                else:
                    size1 = self.get_size(remote)
                    size2 = os.path.getsize(local)
                    if size1 != size2:
                        print(f"size difference: {abs(size1-size2)}")
                        a = input("Delete and redownload?(Y/N) ")
                        if a == "Y":
                            os.remove(local)
                            then = time.time()
                            conn.get(remote, local)
                            size = os.path.getsize(local)
                            metrics_output(then, size, local)
                        else:
                            print("Continuing")
                    print(f"File already exists {local}")
            return
        elif self.isdir(remote):
            if not os.path.exists(local) or os.path.isfile(local):
                if os.path.isfile(local):
                    os.remove(local)
                print(f"creating new local directory {local}")
                os.mkdir(local)
                os.chown(local, uid=1000, gid=1000)
            pathlist = self.listdir(remote)
            while len(pathlist) > 0:
                chosen = random.choice(pathlist)
                pathlist.remove(chosen)
                full_local = os.path.join(local, chosen)
                full_remote = os.path.join(remote, chosen)
                self._traverse(full_local, full_remote)

    def sync(self):
        local = self.local
        remote = self.remote
        self._traverse(local, remote)


def metrics_output(then, size, path):
    diff = time.time() - then
    bytes_per_second = size / diff
    bases = ["Bytes", "KB", "MB", "GB"]
    scales = {
        "Bytes": 0,
        "KB": 1000,
        "MB": 1000000,
        "GB": 1000000000
    }
    index = 0
    print(bytes_per_second)
    while bytes_per_second > scales[bases[index]]:
        index += 1
        if index >= len(bases):
            break
    if index in [0,1]:
        amount = str(bytes_per_second) + " bytes/sec"
    else:
        amount = f"{bytes_per_second / scales[bases[index - 1]]} {bases[index - 1]}/sec"
    filename = os.path.basename(path)
    print(f"<-Finished  {filename} : {size} || {amount} ->")








if __name__ == "__main__":
    connection = Connection(host,port)
    connection.sync()
