import time
import socket
import re
import logging

logger = logging.getLogger(__name__)

OOB = 0x1
PORT = 21
MAXSIZE = 2**24

CRLF = '\r\n'
B_CRLF = b'\r\n'


class Memo:

    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, path, instance=None):
        if instance is not None:
            self.instance = instance
        if path in self.cache:
            return self.cache[path]
        else:
            func = self.func
            instance = self.instance
            results = func(instance, path)
            self.cache[path] = results
            return results


class Stats:

    processed = 0
    downloaded = 0
    total = 0
    skipped = 0
    replaced = 0
    start = time.time()
    last = None

    def calc_speed(self, path, size, starttime):
        diff = time.time() - starttime
        denom = "bytes"
        tiers = {
            "KiB": 2**20,
            "MiB": 2**30,
            "GiB": 2**40,
        }
        for k,v in tiers.items():
            if size < v:
                denom = k
                break
        val = size / diff
        self.last = path
        self.downloaded += 1
        self.total += size
        if not self.downloaded % 10:
            logger.info("Complete: %s; %f %s/sec. Total: %d", path, val, denom, size)

    def log_report(self):
        msg = f"Elapsed Time: {time.time() - self.start}; Processed: {self.processed}; Total: {self.total}; Downloaded: {self.downloaded}; Skipped: {self.skipped};\r"
        logger.info(msg)


class FTPClient:

    host = ''
    port = PORT
    maxsize = MAXSIZE
    timeout = 999
    sock = None
    remote = None
    passivemode = True
    trust_pasv_ipv4 = True

    def __init__(self, source_address=None, encoding='utf8', timeout=999):
        self.stats = Stats()
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
        logger.info(message)
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

    def putcmd(self, line):
        value = self.putline(line)
        return value

    def putline(self, line):
        if '\r' in line or '\n' in line:
            raise Exception('an illegal newline character shouldn not be contained')
        line = line + CRLF
        self.sock.sendall(line.encode(self.encoding))


    def voidresp(self):
        resp = self.getresp()
        if resp[:1] != '2':
            logger.debug(resp)
            raise Exception(resp)
        return resp

    def abort(self):
        line = b'ABOR' + B_CRLF
        self.sock.sendall(line, OOB)
        resp = self.getmultiline()
        if resp[:3] not in ['426', '225', '226']:
            logger.debug(resp)
            raise Exception(resp)
        return resp

    def sendcmd(self, cmd):
        self.putcmd(cmd)
        resp = self.getresp()
        return resp

    def voidcmd(self, cmd):
        self.putcmd(cmd)
        void = self.voidresp()
        return void

    def sendport(self, host, port):
        hbits = host.split('.')
        pbits = [repr(port//256), repr(port%256)]
        bits = hbits + pbits
        cmd = 'PORT ' + ','.join(bits)
        void = self.voidcmd(cmd)
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
        logging.info(resp)
        sock.settimeout(self.timeout)
        return sock

    def sendeprt(self, host, port):
        if self.af == socket.AF_INET: af = 1
        if self.af == socket.AF_INET6: af = 2
        if af == 0: raise Exception('unsupported address family')
        fields = ['', repr(af), host, repr(port), '']
        cmd = 'EPRT ' + '|'.join(fields)
        resp = self.voidcmd(cmd)
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

    def login(self, user = '', passwd = '', acct = ''):
        self.user = user
        self.passwd = passwd
        resp = self.sendcmd('USER ' + user)
        if resp[0] == '3': resp = self.sendcmd('PASS ' + passwd)
        if resp[0] != '2': raise Exception(resp)
        return resp

    def retrbinary(self, cmd, callback, blocksize=MAXSIZE, rest=None):
        self.voidcmd('TYPE I')
        conn = self.transfercmd(cmd, rest)
        total = 0
        while True:
            data = conn.recv(blocksize)
            if not data:
                break
            callback(data)
            total += len(data)
        resp = self.voidresp()
        logger.info(resp)
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
        val = self.voidresp()
        return val

    def storbinary(self, cmd, fp, blocksize=8192, callback=None, rest=None):
        self.voidcmd('TYPE I')
        with self.transfercmd(cmd, rest) as conn:
            while 1:
                buf = fp.read(blocksize)
                if not buf:
                    break
                conn.sendall(buf)
                if callback:
                    callback(buf)
        return self.voidresp()

    def storlines(self, cmd, fp, callback=None):
        self.voidcmd('TYPE A')
        with self.transfercmd(cmd) as conn:
            while 1:
                buf = fp.readline(self.maxline + 1)
                if len(buf) > self.maxline:
                    raise Exception("got more than %d bytes" % self.maxline)
                if not buf:
                    break
                if buf[-2:] != B_CRLF:
                    if buf[-1] in B_CRLF: buf = buf[:-1]
                    buf = buf + B_CRLF
                conn.sendall(buf)
                if callback:
                    callback(buf)
        return self.voidresp()

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
            ref.append((name, entry))
        return ref

    def cwd(self, dirname):
        if dirname == '..':
            try:
                return self.voidcmd('CDUP')
            except Exception as msg:
                if msg.args[0][:3] != '500':
                    raise
        elif dirname == '':
            dirname = '.'
        cmd = 'CWD ' + dirname
        return self.voidcmd(cmd)

    def size(self, filename):
        resp = self.sendcmd('SIZE ' + filename)
        if resp[:3] == '213':
            s = resp[3:].strip()
            return int(s)

    def pwd(self):
        resp = self.voidcmd('PWD')
        if not resp.startswith('257'):
            return ''
        return parse257(resp)

    def quit(self):
        resp = self.voidcmd('QUIT')
        logger.info(resp)
        self.close()
        return resp

    def close(self):
        try:
            file = self.file
            self.file = None
            if file is not None:
                file.close()
        finally:
            sock = self.sock
            self.sock = None
            if sock is not None:
                sock.close()
        return

    @Memo
    def listdir(self, path=None, instance=None):
        if not path:
            path = '.'
        results = self.mlsd(path)
        filelist = [i[0] for i in results]
        return filelist, dict(results)

    def isdir(self, path, instance=None):
        instance = instance if instance else self
        if path in ['.', '..']:
            return True
        results = self.listdir(path, instance=instance)
        if results:
            filelist, info = results
            if len(filelist) == 1 and '..' not in filelist:
                return False
        return True

    def isfile(self, path):
        return not self.isdir(path, instance=self)

    def get(self, remote, local):
        client = FTPClient()
        client.connect(self.host, self.port)
        client.login(self.user, self.passwd)
        cmd = "RETR " + remote
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
