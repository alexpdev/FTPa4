from threading import Thread
import os
import time
import logging

logger = logging.getLogger(__name__)

class Pool:
    def __init__(self, maxsize=3):
        self.threads = {}
        self.maxsize = maxsize

    def add(self, thread, total):
        least = None
        removals = []
        for t, size in self.threads.items():
            if t.is_alive():
                if least is None or size < least:
                    least = size
            else:
                removals.append(thread)
        for t in removals:
            del self.threads[t]
        if len(self.threads) >= self.maxsize:
            for k,v in self.threads.items():
                if v == least:
                    k.join()
                    del self.threads[k]
                    break
        self.threads[thread] = total
        thread.start()

class MyThread(Thread):
    def __init__(self, remote, local, client):
        super().__init__()
        self.local = local
        self.remote = remote
        self.client = client

    def run(self):
        self.client.get(self.remote, self.local)

def traverse(remote, local, client, pool):
    client.stats.processed += 1
    if client.stats.processed % 50 == 0:
        client.print_stats()
    if client.isfile(remote):
        size = client.getsize(remote)
        if os.path.exists(local):
            if os.path.getsize(local) >= int(size):
                client.stats.skipped += 1
                logger.info("Skipping: %s" % remote)
                return
            else:
                os.remove(local)
                client.stats.replaced += 1
        thread = MyThread(remote, local, client)
        pool.add(thread, size)
    else:
        if not os.path.exists(local):
            os.mkdir(local)
        lst = client.listdir(remote)
        for path in lst:
            if path in ['.', '..']:
                continue
            remote1 = os.path.join(remote, path).replace('\\','/')
            local1 = os.path.join(local, path).replace('\\','/')
            traverse(remote1, local1, client, pool)
