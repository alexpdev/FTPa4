import socket
import asyncio
import re
import logging

logger = logging.getLogger(__name__)

OOB = 0x1
PORT = 21
MAXSIZE = 2**20

CRLF = '\r\n'
B_CRLF = b'\r\n'

class AsyncFTP:

    host = ''
    port = PORT
    maxsize = MAXSIZE
    timeout = 999
    sock = None
    remote = None
    passivemode = True
    trust_pasv_ipv4 = True

    def __init__(self, source_address=None, encoding='utf8'):
        self.loop = asyncio.get_event_loop()
        self.encoding = encoding
        self.source_address = source_address

    async def connect(self, host='', port=0, timeout=None, source_address=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.source_address = source_address
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout, source_address=self.source_address)
        self.af = self.sock.family
        self.file = self.sock.makefile('r', encoding=self.encoding)
        message = await self.getresp()
        logger.info(message)
        return self.sock

    async def getline(self):
        line = self.file.readline(MAXSIZE+1)
        if len(line) > MAXSIZE:
            raise Exception("should have only received one")
        if not line:
            raise EOFError
        if line[-2:] == CRLF:
            line = line[:-2]
        elif line[-1:] in CRLF:
            line = line[:-1]
        logging.info(line)
        return line

    async def getmultiline(self):
        line = await self.getline()
        if line[3:4] == '-':
            code = line[:3]
            while True:
                nextline = await self.getline()
                line += '\n' + nextline
                if nextline[:3] == code and nextline[3:4] != '-':
                    break
        return line

    async def getresp(self):
        resp = await self.getmultiline()
        self.lastresp = resp[:3]
        code = resp[:1]
        if code in '123':
            return resp
        raise Exception(resp)

    async def set_pasv(self, val):
        self.passivesmode = val
        return True

    def _sanatize(self, s):
        if s[:5] in ['pass ', 'PASS ']:
            i = len(s.strip('\r\n'))
            s = s[:5] + '*'*(i-5) + s[i:]
        return repr(s)

    async def putcmd(self, line):
        value = await self.putline(line)
        return value

    async def putline(self, line):
        if '\r' in line or '\n' in line:
            raise Exception('an illegal newline character shouldn not be contained')
        line = line + CRLF
        logging.info(line)
        self.sock.sendall(line.encode(self.encoding))
        return True

    async def voidresp(self):
        resp = await self.getresp()
        if resp[:1] != '2':
            raise Exception(resp)
        return resp

    async def abort(self):
        line = b'ABOR' + B_CRLF
        self.sock.sendall(line, OOB)
        resp = await self.getmultiline()
        if resp[:3] not in ['426', '225', '226']:
            raise Exception(resp)
        return resp

    async def sendcmd(self, cmd):
        await self.putcmd(cmd)
        resp = await self.getresp()
        return resp

    async def voidcmd(self, cmd):
        await self.putcmd(cmd)
        void = await self.voidresp()
        return void

    async def sendport(self, host, port):
        hbits = host.split('.')
        pbits = [repr(port//256), repr(port%256)]
        bits = hbits + pbits
        cmd = 'PORT ' + ','.join(bits)
        void = await self.voidcmd(cmd)
        return void

    async def makeport(self):
        sock = socket.create_server(('', 0), family=self.af, backlog=1)
        port = sock.getsockname()[1]
        host = self.sock.getsockname()[0]
        if self.af == socket.AF_INET:
            resp = await self.sendport(host, port)
        else:
            resp = await self.sendeprt(host, port)
        logging.info(resp)
        sock.settimeout(self.timeout)
        return sock

    async def sendeprt(self, host, port):
        if self.af == socket.AF_INET: af = 1
        if self.af == socket.AF_INET6: af = 2
        if af == 0: raise Exception('unsupported address family')
        fields = ['', repr(af), host, repr(port), '']
        cmd = 'EPRT ' + '|'.join(fields)
        resp = await self.voidcmd(cmd)
        return resp

    async def makepasv(self):
        if self.af == socket.AF_INET:
            val = await self.sendcmd('PASV')
            coro = await parse227(val)
            _, port = coro
            host = self.sock.getpeername()[0]
        else:
            host, port = await parse229(self.sendcmd('EPSV'), self.sock.getpeername())
        return host, port

    async def ntransfercmd(self, cmd, rest=None):
        size = None
        if self.passivemode:
            coro = await self.makepasv()
            host, port = coro
            conn = socket.create_connection((host, port), self.timeout, source_address=self.source_address)
            try:
                if rest is not None:
                    await self.sendcmd("REST %s" % rest)
                resp = await self.sendcmd(cmd)
                if resp[0] == '2':
                    resp = await self.getresp()
                if resp[0] != '1':
                    raise Exception(resp)
            except:
                conn.close()
                raise
        else:
            sock = await self.makeport()
            if rest is not None:
                await self.sendcmd("REST %s" % rest)
            resp = await self.sendcmd(cmd)
            if resp[0] == '2':
                resp = await self.getresp()
            if resp[0] != '1':
                raise Exception(resp)
            conn, _ = sock.accept()
            conn.settimeout(self.timeout)
        if resp[:3] == '150':
            size = await parse150(resp)
        return conn, size

    async def transfercmd(self, cmd, rest=None):
        val = await self.ntransfercmd(cmd, rest)
        return val[0]

    async def login(self, user = '', passwd = '', acct = ''):
        resp = await self.sendcmd('USER ' + user)
        if resp[0] == '3': resp = await self.sendcmd('PASS ' + passwd)
        if resp[0] != '2': raise Exception(resp)
        return resp

    async def retrbinary(self, cmd, callback, blocksize=MAXSIZE, rest=None):
        await self.voidcmd('TYPE I')
        conn = await self.transfercmd(cmd, rest)
        while True:
            data = conn.recv(blocksize)
            if not data:
                break
            callback(data)
            await asyncio.sleep(0)
        resp = await self.voidresp()
        return resp

    async def retrlines(self, cmd, callback):
        if callback is None: callback = print
        resp = await self.sendcmd('TYPE A')
        conn = await self.transfercmd(cmd)
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
        val = await self.voidresp()
        return val

    async def storbinary(self, cmd, fp, blocksize=8192, callback=None, rest=None):
        await self.voidcmd('TYPE I')
        with await self.transfercmd(cmd, rest) as conn:
            while 1:
                buf = fp.read(blocksize)
                if not buf:
                    break
                conn.sendall(buf)
                if callback:
                    callback(buf)
        return await self.voidresp()

    async def storlines(self, cmd, fp, callback=None):
        await self.voidcmd('TYPE A')
        with await self.transfercmd(cmd) as conn:
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
        return await self.voidresp()

    async def acct(self, password):
        cmd = 'ACCT ' + password
        return await self.voidcmd(cmd)

    async def nlst(self, *args):
        cmd = 'NLST'
        for arg in args:
            cmd = cmd + (' ' + arg)
        files = []
        await self.retrlines(cmd, files.append)
        return files

    async def dir(self, *args):
        cmd = 'LIST'
        func = None
        if args[-1:] and not isinstance(args[-1], str):
            args, func = args[:-1], args[-1]
        for arg in args:
            if arg:
                cmd = cmd + (' ' + arg)
        await self.retrlines(cmd, func)

    async def mlsd(self, path="", facts=[]):
        if facts:
            await self.sendcmd("OPTS MLST " + ";".join(facts) + ";")
        if path:
            path = path.replace('\\', '/')
            cmd = "MLSD %s" % path
        else:
            cmd = "MLSD"
        lines = []
        await self.retrlines(cmd, lines.append)
        ref = []
        for line in lines:
            facts_found, _, name = line.rstrip(CRLF).partition(' ')
            entry = {}
            for fact in facts_found[:-1].split(";"):
                key, _, value = fact.partition("=")
                entry[key.lower()] = value
            ref.append((name, entry))
        return ref

    async def rename(self, fromname, toname):
        resp = await self.sendcmd('RNFR ' + fromname)
        if resp[0] != '3':
            raise Exception(resp)
        return await self.voidcmd('RNTO ' + toname)

    async def delete(self, filename):
        resp = await self.sendcmd('DELE ' + filename)
        if resp[:3] in {'250', '200'}:
            return resp
        else:
            raise Exception(resp)

    async def cwd(self, dirname):
        if dirname == '..':
            try:
                return await self.voidcmd('CDUP')
            except Exception as msg:
                if msg.args[0][:3] != '500':
                    raise
        elif dirname == '':
            dirname = '.'
        cmd = 'CWD ' + dirname
        return await self.voidcmd(cmd)

    async def size(self, filename):
        resp = await self.sendcmd('SIZE ' + filename)
        if resp[:3] == '213':
            s = resp[3:].strip()
            return int(s)

    async def mkd(self, dirname):
        resp = await self.voidcmd('MKD ' + dirname)
        if not resp.startswith('257'):
            return ''
        return await parse257(resp)

    async def rmd(self, dirname):
        return await self.voidcmd('RMD ' + dirname)

    async def pwd(self):
        resp = await self.voidcmd('PWD')
        if not resp.startswith('257'):
            return ''
        return await parse257(resp)

    async def quit(self):
        resp = await self.voidcmd('QUIT')
        await self.close()
        return resp

    async def close(self):
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

    async def listdir(self, path):
        data = await self.mlsd(path=path)
        itemlist = [i[0] for i in data]
        data = dict(data)
        return itemlist, data

    async def get(self, targ, dest):
        cmd = "RETR " + targ
        dest = open(dest, 'ab')
        await self.retrbinary(cmd, lambda x: dest.write(x))
        return True



class rx:
    _150_re = None
    _227_re = None

async def parse150(resp):
    if resp[:3] != '150': raise Exception(resp)
    if rx._150_re is None: rx._150_re = re.compile(r"150 .* \((\d+) bytes\)", re.IGNORECASE | re.ASCII)
    m = rx._150_re.match(resp)
    if not m: return None
    return int(m.group(1))

async def parse227(resp):
    if resp[:3] != '227':  raise Exception(resp)
    if rx._227_re is None: rx._227_re = re.compile(r'(\d+),(\d+),(\d+),(\d+),(\d+),(\d+)', re.ASCII)
    m = rx._227_re.search(resp)
    if not m:  raise Exception(resp)
    numbers = m.groups()
    host = '.'.join(numbers[:4])
    port = (int(numbers[4]) << 8) + int(numbers[5])
    return host, port

async def parse229(resp, peer):
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

async def parse257(resp):
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
