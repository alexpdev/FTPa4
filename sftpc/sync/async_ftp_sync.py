import time
import asyncio
import aioftp
import os
import dotenv


class AFTP:

    def __init__(self, client):
        self.client = client
        self.count = 0
        self.download = 0
        self.already_had = 0
        self.max_tasks = []

    def print_stats(self):
        msg = [
            f"Processed: {self.count}",
            f"Downloading: {self.download}",
            f"Exist: {self.already_had}"
        ]
        print('\t'.join(msg), end='\r')

    async def get_stream(self, path):
        try:
            stream = await self.client.download_stream(path)
        except Exception as err:
            self.clear_tasks()
            return None
        return stream

    @staticmethod
    def clear_tasks():
        for task in asyncio.current_task(asyncio.get_running_loop()):
            task.cancel()

    async def get_file(self, local, remote, size):
        self.download += 1
        self.print_stats()
        stream = await self.get_stream(remote)
        await self.write_blocks(stream, local)
        await stream.finish()
        then = time.time()
        metrics_output(then, size, local)
        return

    @staticmethod
    def write_block(upload_file_path, block):
        open(upload_file_path, 'ab').write(block)

    async def write_blocks(self, stream, upload_file_path):
        async for block in stream.iter_by_block():
            self.write_block(upload_file_path, block)

    async def traverse(self, local, remote):
        if await self.client.exists(remote):
            if await self.client.is_file(remote):
                self.count += 1
                self.print_stats()
                info = await self.client.stat(remote)
                if os.path.exists(local):
                    size2 = os.path.getsize(local)
                    if int(info['size']) >= int(size2):
                        self.already_had += 1
                    return
                await self.get_file(local, remote, int(info['size']))
                self.print_stats()
                return
            elif await self.client.is_dir(remote):
                print(local, remote)
                if not os.path.exists(local) or os.path.isfile(local):
                    if os.path.isfile(local):
                        os.remove(local)
                    print(f"creating new local directory {local} from {remote}")
                    os.mkdir(local)
                    os.chown(local, uid=1000, gid=1000)
                pathlist = await self.client.list(remote)
                for path, info in pathlist:
                    if path.name not in [".", "..", os.path.basename(remote)]:
                        await self.traverse(os.path.join(local, path.name), path)
                return
            return
        return

def metrics_output(then, size, path):
    diff = time.time() - then
    bytes_per_second = size / diff
    scales = {
        "Bytes": 1024,
        "KB": 2**20,
        "MB": 2**30,
        "GB": 2**40
    }
    for k, v in scales.items():
        if bytes_per_second < v:
            break
    amount = f"{bytes_per_second} {k}/sec"
    filename = os.path.basename(path)
    print(f"<-Finished  {filename} : {size} || {amount} ->")


async def asyncftp(host, port, login, password, local, remote):
    async with aioftp.Client.context(host, port, login, password) as conn:
        client = AFTP(conn)
        await client.traverse(local, remote)
    return

def main(host, port, login, password, local, remote):
    asyncio.run(asyncftp(host, port, login, password, local, remote))
    print()
