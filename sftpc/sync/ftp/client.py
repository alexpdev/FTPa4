#! /usr/bin/python3
# -*- coding: utf-8 -*-

import os
from ftplib import FTP

class FTPClient:
    """
     FTP Client Implementation
    """

    def __init__(self, host=None, port=None, user=None, passwd=None):
        self._host = host
        self._port = port
        self._user = user
        self._passwd = passwd
        self._mode = "bin"
        self._local_path = "."
        self._remote_path = "."

    def open(self,host,port=21):
        self._host = host
        self._port = port

    def set_local_path(self, path):
        self._local_path = path

    def cd(self, path=None):
        if not path:
            return

        if path.startswith(".."):
            self.cdup()
            if len(path) == 2:
                return
            path = path[3:]

        if path[0] == "/" or path[0] == "\\":
            self._remote_path = path

        else:
            self._remote_path += "/" + path

    def cdup(self):
        self._remote_path = os.path.split(self._remote_path)[0]

    def append(self,txt,local):
        with open(local,"ta") as fp:
            fp.write(txt)
        return

    def set_ascii(self):
        self._mode = "ascii"

    def set_binary(self):
        self._mode = "bin"

    def set_mode(self, mode):
        text_mode = ["t", "a", "ascii", "text"]
        bin_mode = ["b", "bin", "bytes", "binary"]
        if mode in text_mode:
            self.set_ascii()
        elif mode in bin_mode:
            self.set_binary()

    def ls(self,path=None):
        if not path:
            path = self._remote
        ftp = self.get_client()
        ftp.dir(path)
        lst = ftp.nlst(path)
        ftp.close()
        return lst

    lst = ls

    def get_client(self):
        if not self._port:
            self._port = 21
        ftp = FTP()
        ftp.connect(host=self._host,port=self._port)
        print("Connect Successfull")
        ftp.login(user=self._user,passwd=self._passwd)
        print("Login Successfull")
        ftp.cwd(self._remote)
        return ftp

    def close(self):
        print("Connection Closed Successfully")
        return

    def disconnect(self):
        return self.close()

    def bye(self):
        return self.disconnect()

    def delete(self,path):
        ftp = self.get_client()
        if isinstance(path,str):
            ftp.delete(path)
        else:
            for fp in path:
                ftp.delete(fp)
        return ftp.close()

    def mdelete(self,paths):
        return self.delete(paths)

    def lcd(self,path=None):
        return self.cd(path)

    def mls(self,paths):
        for path in paths:
            yield self.ls(path)

    def pwd(self):
        print(self._remote)

    def quit(self):
        return self.bye()

    def user(self,user=None,passwd=None):
        self._user = user
        self._passwd = passwd

    def size(self,fp,*args,**kwargs):
        ftp = self.get_client()
        return ftp.size(fp,*args,**kwargs)

    def ftype(self,mode=None):
        if not mode:
            print(self._mode)
            return self._mode
        self._mode = mode

    def get(self, src, dest=None):
        ftp = self.get_client()
        if not dest:
            name = os.path.split(src)[-1]
            dest = os.path.join(os.getcwd(),name)
        if self._mode == "ascii":
            with open(dest,"wt") as textfile:
                ftp.retrlines(f'RETR {src}',textfile.write)
        else:
            with open(dest,"wb") as binfile:
                ftp.retrbinary(f"RETR {src}", binfile.write)
        print("transfer complete")
        return ftp.close()

    def mget(self,lst):
        for path in lst:
            self.get(path)
        return

    def put(self, src, dest=None):
        ftp = self.get_client()
        data = open(src, 'rt')
        if not dest:
            name = os.path.split(src)[-1]
            dest = os.path.join(os.getcwd(),name)
        if self._mode == "ascii":
            ftp.storlines(f'STOR {data}',dest)
        else:
            ftp.storbinary("STOR {data}", dest)
        print("completed transfer")
        return ftp.close()

    def mput(self,srclst,*args,**kwargs):
        for path in srclst:
            self.put(path,None)
        return
