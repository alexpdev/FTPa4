import asyncio
import asyncssh
import os
import random
import time
import dotenv

dotenv.load_dotenv()
host = os.environ["HN"]
port = int(os.environ["PT"])
un = os.environ["UN"]
pw = os.environ["PW"]
LOCAL = "/data/torrents/remote"
REMOTE = "."

class SFTP:

    def __init__(self, client):
        self.client = client
        self.count = 0
        self.download = 0
        self.already_had = 0

    def print_stats(self):
        msg = [
            f"Processed: {self.count}",
            f"Downloading: {self.download}",
            f"Exist: {self.already_had}"
        ]
        print('\t'.join(msg), end='\r')

    async def get_file(self, local, remote, size):
        self.download += 1
        self.print_stats()
        then = time.time()
        await self.client.get(remote, local)
        metrics_output(then, size, local)
        return

    async def traverse(self, local, remote):
        self.count += 1
        self.print_stats()
        if await self.client.exists(remote):
            if await self.client.isfile(remote):
                size1 = await self.client.getsize(remote)
                if os.path.exists(local):
                    size2 = os.path.getsize(local)
                    if size1 >= size2:
                        self.already_had += 1
                        self.print_stats()
                        return
                await self.get_file(local, remote, size1)
                return
            elif await self.client.isdir(remote):
                if not os.path.exists(local) or os.path.isfile(local):
                    if os.path.isfile(local):
                        os.remove(local)
                    print(f"creating new local directory {local} from {remote}")
                    os.mkdir(local)
                    os.chown(local, uid=1000, gid=1000)
                pathlist = await self.client.listdir(remote)
                pathlist2 = []
                while len(pathlist) > 0:
                    chosen = random.choice(pathlist)
                    if chosen in [".", ".."]:
                        pathlist.remove(chosen)
                        continue
                    pathlist.remove(chosen)
                    full_local = os.path.join(local, chosen)
                    full_remote = os.path.join(remote, chosen)
                    pathlist2.append((full_local, full_remote))
                futures = [*(asyncio.create_task(self.traverse(*paths))
                    for paths in pathlist2)]
                if futures:
                    await asyncio.wait(futures)
        return

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
    print("\n", bytes_per_second)
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

async def run_client():
    print(host, port, un, pw)
    async with asyncssh.connect(host=host, port=port, username=un, password=pw, known_hosts=None) as conn:
        print(conn)
        async with conn.start_sftp_client() as sftp:
            client = SFTP(sftp)
            await client.traverse(LOCAL, REMOTE)
    return

if __name__ == "__main__":
    asyncio.run(run_client())
    print()
