from queue import Queue
from threading import Thread
import os
import logging

logger = logging.getLogger(__name__)

class MyThread(Thread):
    def __init__(self, remote, local, client):
        super().__init__()
        self.local = local
        self.remote = remote
        self.client = client

    def run(self):
        self.client.get(self.remote, self.local)

class Traverse(Thread):
    def __init__(self, local, remote, client, queue):
        super().__init__()
        self.local = local
        self.remote = remote
        self.client = client
        self.queue = queue

    def traverse(self, local, remote):
        self.client.stats.processed += 1
        lst = self.client.listdir(remote)
        if len(lst) == 1 and lst[0].isfile():
            remote = lst[0]
            size = remote.get_size()
            if os.path.exists(local):
                if os.path.getsize(local) >= int(size):
                    self.client.stats.skipped += 1
                    if self.client.stats.skipped % 10 == 0:
                        logger.info("Skipping: %s" % remote)
                    return
                else:
                    if os.path.isfile(local):
                        os.remove(local)
                    else:
                        os.rmdir(local)
                    self.client.stats.replaced += 1
            self.queue.put((local, remote))
        else:
            if not os.path.exists(local):
                os.mkdir(local)
            for path in lst:
                if path.name in ['.', '..']:
                    continue
                remote1 = path.path
                local1 = os.path.join(local, path.name).replace('\\','/')
                self.traverse(local1, remote1)

    def run(self):
        self.traverse(self.local, self.remote)


class SyncDir:
    def __init__(self, local, remote, client):
        self.fifo = Queue()
        self.remote_root = remote
        self.local_root = local
        self.client = client
        self.walker = Traverse(
            self.local_root, self.remote_root, self.client, self.fifo
        )

    def traverse(self):
        self.walker.run()

    def run(self):
        while not self.fifo.empty() or self.walker.is_alive():
            try:
                local, remote = self.fifo.get(timeout=5)
                print(f'Getting {local}, {remote}')
                self.client.get(remote, local)
                self.fifo.task_done()
            except:
                print("Something went wrong")
                continue
        print("Empty Queue")
