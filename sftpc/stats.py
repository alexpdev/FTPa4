import logging
import time

logger = logging.getLogger(__file__)

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
        return suffix

    def humanize(self, size, starttime):
        interval = time.time() - starttime
        num = size / interval
        suffix = self.byte_suffix(num)
        return "{0:.2f} {1}/s".format(num, suffix)

    def calc_speed(self, path, size, starttime):
        speed = self.humanize(size, starttime)
        self.last = path
        self.downloaded += 1
        self.total += size
        print(f"Complete: {path}; Size: {size}; Rate {speed}")

    def log_report(self):
        msg = f"Elapsed Time: {time.time() - self.start}; Processed: {self.processed}; Total: {self.total}; Downloaded: {self.downloaded}; Skipped: {self.skipped};"
        logger.debug(msg)
        print(msg, end='\r')

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
