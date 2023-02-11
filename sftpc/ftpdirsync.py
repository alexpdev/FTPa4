import os
import time
import socket
import re
import logging


class PathIO:
    def __init__(self, name, parent, **kwargs):
        self.name = name
        self.parent = parent
        self.args = kwargs
        self.path = os.path.join(parent, name).replace('\\','/')

    def isdir(self):
        if self.args['type'] == 'file':
            return False
        return True

    def isfile(self):
        return not self.isdir()

    def get_size(self):
        if self.isfile():
            return self.args['size']
        else:
            return self.args['sizd']

    def __repr__(self):
        return f'<PathIO {self.name};{self.args["type"]}>'


class StatCollector:

    processed = 0
    downloaded = 0
    total = 0
    skipped = 0
    replaced = 0
    start = time.time()
    last = None
    GiB = 1 << 30
    MiB = 1 << 20
    KiB = 1 << 10

    def byte_suffix(self, size):
        """
        Return a human representation of a number of bytes.

        Parameters
        ----------
        b : int
            number of bytes
        """
        abbrevs = (
            (self.GiB, 'GiB'),
            (self.MiB, 'MiB'),
            (self.KiB, 'KiB'),
            (1, 'B')
        )
        for factor, suffix in abbrevs:
            if size >= factor:
                break
        return factor, suffix

    def humanize(self, size, starttime):
        interval = time.time() - starttime
        num = size / interval
        factor, suffix = self.byte_suffix(num)
        return "{0:.2f} {1}/s".format(num / factor, suffix)

    def calc_speed(self, path, size, starttime):
        speed = self.humanize(size, starttime)
        self.last = path.name
        self.downloaded += 1
        self.total += size
        print(f"Complete: {path}; Size: {size}; Rate {speed}")

    def log_report(self):
        msg = f"Elapsed Time: {time.time() - self.start} seconds; Processed: {self.processed}; Total: {self.total}; Downloaded: {self.downloaded}; Skipped: {self.skipped};"
        logger.debug(msg)
        print(msg, end='')

    def show_end(self):
        span = time.time() - self.start
        rate = self.humanize(self.total, self.start)
        stats = {
            "time": span,
            "processed": self.processed,
            "total": self.total,
            "downloaded": self.downloaded,
            "skipped": self.skipped,
            'avg rate': rate
        }
        print(stats)


logger = logging.getLogger(__name__)

OOB = 0x1
PORT = 21
MAXSIZE = 2**24

CRLF = '\r\n'
B_CRLF = b'\r\n'


class Client:

    host = ''
    port = PORT
    maxsize = MAXSIZE
    timeout = 999
    sock = None
    remote = None
    passivemode = True
    trust_pasv_ipv4 = True

    def __init__(self, source_address=None, encoding='utf8', timeout=999):
        self.stats = StatCollector()
        self.encoding = encoding
        self.source_address = source_address
        self.timeout = timeout

    def connect(self, host='', port=0):
        self.host = host
        self.port = port
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout, source_address=self.source_address)
        self.af = self.sock.family
        self.file = self.sock.makefile('r', encoding=self.encoding)
        message = self.getresp()
        logger.debug(message)
        return self.sock

    def getline(self):
        line = self.file.readline(MAXSIZE+1)
        if not line:
            raise EOFError
        if line[-2:] == CRLF:
            line = line[:-2]
        elif line[-1:] in CRLF:
            line = line[:-1]
        logger.debug(line)
        return line

    def getmultiline(self):
        line = self.getline()
        if line[3:4] == '-':
            code = line[:3]
            while True:
                nextline = self.getline()
                line += '\n' + nextline
                if nextline[:3] == code and nextline[3:4] != '-':
                    break
        return line

    def getresp(self):
        resp = self.getmultiline()
        self.lastresp = resp[:3]
        code = resp[:1]
        if code in '123':
            return resp
        logger.debug(resp)
        raise Exception(resp)

    def set_pasv(self, val):
        self.passivesmode = val
        return True

    def _sanatize(self, s):
        if s[:5] in ['pass ', 'PASS ']:
            i = len(s.strip('\r\n'))
            s = s[:5] + '*'*(i-5) + s[i:]
        return repr(s)

    def putline(self, line):
        if '\r' in line or '\n' in line:
            raise Exception('an illegal newline character shouldn not be contained')
        line = line + CRLF
        self.sock.sendall(line.encode(self.encoding))

    def abort(self):
        line = b'ABOR' + B_CRLF
        self.sock.sendall(line, OOB)
        resp = self.getmultiline()
        if resp[:3] not in ['426', '225', '226']:
            logger.debug(resp)
            raise Exception(resp)
        return resp

    def sendcmd(self, cmd):
        self.putline(cmd)
        resp = self.getresp()
        return resp

    def sendport(self, host, port):
        hbits = host.split('.')
        pbits = [repr(port//256), repr(port%256)]
        bits = hbits + pbits
        cmd = 'PORT ' + ','.join(bits)
        void = self.sendcmd(cmd)
        logger.debug(void)
        return void

    def makeport(self):
        sock = socket.create_server(('', 0), family=self.af, backlog=1)
        port = sock.getsockname()[1]
        host = self.sock.getsockname()[0]
        if self.af == socket.AF_INET:
            resp = self.sendport(host, port)
        else:
            resp = self.sendeprt(host, port)
        logging.debug(resp)
        sock.settimeout(self.timeout)
        return sock

    def sendeprt(self, host, port):
        if self.af == socket.AF_INET: af = 1
        if self.af == socket.AF_INET6: af = 2
        if af == 0: raise Exception('unsupported address family')
        fields = ['', repr(af), host, repr(port), '']
        cmd = 'EPRT ' + '|'.join(fields)
        resp = self.sendcmd(cmd)
        return resp

    def makepasv(self):
        if self.af == socket.AF_INET:
            val = self.sendcmd('PASV')
            coro = parse227(val)
            _, port = coro
            host = self.sock.getpeername()[0]
        else:
            host, port = parse229(self.sendcmd('EPSV'), self.sock.getpeername())
        return host, port

    def ntransfercmd(self, cmd, rest=None):
        size = None
        if self.passivemode:
            coro = self.makepasv()
            host, port = coro
            conn = socket.create_connection((host, port), self.timeout, source_address=self.source_address)
            try:
                if rest is not None:
                    self.sendcmd("REST %s" % rest)
                resp = self.sendcmd(cmd)
                if resp[0] == '2':
                    resp = self.getresp()
                if resp[0] != '1':
                    raise Exception(resp)
            except:
                conn.close()
                raise
        else:
            sock = self.makeport()
            if rest is not None:
                self.sendcmd("REST %s" % rest)
            resp = self.sendcmd(cmd)
            if resp[0] == '2':
                resp = self.getresp()
            if resp[0] != '1':
                raise Exception(resp)
            conn, _ = sock.accept()
            conn.settimeout(self.timeout)
        if resp[:3] == '150':
            size = parse150(resp)
        return conn, size

    def transfercmd(self, cmd, rest=None):
        val = self.ntransfercmd(cmd, rest)
        return val[0]

    def login(self, user = '', passwd = ''):
        self.user = user
        self.passwd = passwd
        resp = self.sendcmd('USER ' + user)
        if resp[0] == '3': resp = self.sendcmd('PASS ' + passwd)
        if resp[0] != '2': raise Exception(resp)
        return resp

    def retrbinary(self, cmd, callback, blocksize=MAXSIZE, rest=None):
        self.sendcmd('TYPE I')
        conn = self.transfercmd(cmd, rest)
        total = 0
        while True:
            data = conn.recv(blocksize)
            if not data:
                break
            callback(data)
            total += len(data)
        resp = self.getresp()
        logger.debug(resp)
        return total

    def retrlines(self, cmd, callback):
        if callback is None: callback = print
        resp = self.sendcmd('TYPE A')
        logger.debug(resp)
        conn = self.transfercmd(cmd)
        fp = conn.makefile('r', encoding=self.encoding)
        while True:
            line = fp.readline(MAXSIZE + 1)
            if len(line) > MAXSIZE:
                raise Exception("got more than %d bytes" % MAXSIZE)
            if not line:
                break
            if line[-2:] == CRLF:
                line = line[:-2]
            elif line[-1:] in CRLF:
                line = line[:-1]
            callback(line)
        val = self.getresp()
        return val

    def nlst(self, *args):
        cmd = 'NLST'
        for arg in args:
            cmd = cmd + (' ' + arg)
        files = []
        self.retrlines(cmd, files.append)
        return files

    def mlsd(self, path="", facts=[]):
        if facts:
            self.sendcmd("OPTS MLST " + ";".join(facts) + ";")
        if path:
            path = path.replace('\\', '/')
            cmd = "MLSD %s" % path
        else:
            cmd = "MLSD"
        lines = []
        self.retrlines(cmd, lines.append)
        ref = []
        for line in lines:
            facts_found, _, name = line.rstrip(CRLF).partition(' ')
            entry = {}
            for fact in facts_found[:-1].split(";"):
                key, _, value = fact.partition("=")
                entry[key.lower()] = value
            path_entry = PathIO(name=name, parent=path, **entry)
            ref.append(path_entry)
        return ref

    def cwd(self, dirname):
        if dirname == '..':
            return self.sendcmd('CDUP')
        elif dirname == '':
            dirname = '.'
        cmd = 'CWD ' + dirname
        return self.sendcmd(cmd)

    def size(self, filename):
        resp = self.sendcmd('SIZE ' + filename)
        if resp[:3] == '213':
            s = resp[3:].strip()
            return int(s)

    def pwd(self):
        resp = self.sendcmd('PWD')
        if not resp.startswith('257'):
            return ''
        return parse257(resp)

    def quit(self):
        resp = self.sendcmd('QUIT')
        logger.debug(resp)
        self.close()
        return resp

    def close(self):
        try: self.file.close()
        except: pass
        try: self.sock.close()
        except: pass

    def getsize(self, path):
        return self.size(path)

    def listdir(self, path="."):
        if isinstance(path, str):
            return self.mlsd(path)
        return self.mlsd(path.path)

    def isdir(self, path):
        if path.isdir():
            return True
        return False

    def isfile(self, path):
        return not self.isdir(path)

    def get(self, remote, local):
        client = Client()
        client.connect(self.host, self.port)
        client.login(self.user, self.passwd)
        cmd = "RETR " + remote.path
        with open(local, 'ab+') as fd:
            callback = lambda x: fd.write(x)
            then = time.time()
            total = client.retrbinary(cmd, callback)
        self.stats.calc_speed(remote, total, then)
        client.quit()

    def print_stats(self):
        self.stats.log_report()

class rx:
    _150_re = None
    _227_re = None

def parse150(resp):
    if resp[:3] != '150': raise Exception(resp)
    if rx._150_re is None: rx._150_re = re.compile(r"150 .* \((\d+) bytes\)", re.IGNORECASE | re.ASCII)
    m = rx._150_re.match(resp)
    if not m: return None
    return int(m.group(1))

def parse227(resp):
    if resp[:3] != '227':  raise Exception(resp)
    if rx._227_re is None: rx._227_re = re.compile(r'(\d+),(\d+),(\d+),(\d+),(\d+),(\d+)', re.ASCII)
    m = rx._227_re.search(resp)
    if not m:  raise Exception(resp)
    numbers = m.groups()
    host = '.'.join(numbers[:4])
    port = (int(numbers[4]) << 8) + int(numbers[5])
    return host, port

def parse229(resp, peer):
    if resp[:3] != '229': raise Exception(resp)
    left = resp.find('(')
    if left < 0: raise Exception(resp)
    right = resp.find(')', left + 1)
    if right < 0:  raise Exception(resp)
    if resp[left + 1] != resp[right - 1]:  raise Exception(resp)
    parts = resp[left + 1:right].split(resp[left+1])
    if len(parts) != 5: raise Exception(resp)
    host = peer[0]
    port = int(parts[3])
    return host, port

def parse257(resp):
    if resp[:3] != '257': raise Exception(resp)
    if resp[3:5] != ' "': return ''
    dirname = ''
    i, n = 5, len(resp)
    while i < n:
        c, i = resp[i], i+1
        if c == '"':
            if i >= n or resp[i] != '"':
                break
            i = i+1
        dirname += c
    return dirname
