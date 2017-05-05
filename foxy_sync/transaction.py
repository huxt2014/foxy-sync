
import os
import sys
import time
import signal
import collections
import concurrent.futures as futures
from threading import Event


JobItem = collections.namedtuple("JobItem",
                                 ["src", "target", "md5", "action", "status",
                                  "info"])


class Transaction:

    # status
    READY = "ready"
    SUCCEED = "succeed"
    FAILED = "failed"

    # action
    PUSH = "push"
    REMOVE = "remove"

    def __init__(self, src_snapshot, target_snapshot, max_workers=5,
                 dump_dir="/tmp"):
        self.src_snapshot = src_snapshot
        self.target_snapshot = target_snapshot
        self.dump_dir = dump_dir
        self.max_workers = max_workers
        self.jobs = self._get_jobs()
        self.futures = []
        self.executor = futures.ThreadPoolExecutor(max_workers)

    def start(self):
        for job in self.jobs:
            # some jobs may already finished, for transaction may be loaded
            # from file.
            if job.status == Transaction.SUCCEED:
                continue
            f = self.executor.submit(self._do_job, job)
            self.futures.append(f)

        while True:
            stop_flag.wait(5)
            if stop_flag.is_set():
                self.cancel_and_exit()

            tmp_futures = []
            failed_number = 0
            for f in self.futures:
                try:
                    if f.result(timeout=0.0) == Transaction.FAILED:
                        failed_number += 1
                except futures.TimeoutError:
                    # ready or running
                    tmp_futures.append(f)

            if not tmp_futures or len(tmp_futures) == failed_number:
                # if all job are finished or remained jobs are all failed
                self.save_and_exit()
            else:
                self.futures = tmp_futures

    def cancel_and_exit(self):
        self.executor.shutdown(wait=False)
        for f in self.futures:
            f.cancel()
        self.dump()
        sys.exit(0)

    def save_and_exit(self):
        self.executor.shutdown()
        self.dump()
        sys.exit(0)

    def dump(self):
        pass

    @classmethod
    def load(cls):
        pass

    def _get_jobs(self):
        raise NotImplementedError

    def _do_job(self, job):
        raise NotImplementedError


class Local2AliOssTransaction(Transaction):

    def _get_jobs(self):
        new_list, removed_list = self.src_snapshot.diff(self.target_snapshot)
        src_root = self.src_snapshot.root
        jobs = []

        for raw, action in ((new_list, Transaction.PUSH),
                            (removed_list, Transaction.REMOVE)):
            jobs += [JobItem(src=os.path.join(src_root, file_id[1]),
                             target=file_id[1],
                             md5=file_id[0],
                             action=action,
                             status=Transaction.READY,
                             info=None)
                     for file_id in raw]
        return jobs

    def _do_job(self, job):
        time.sleep(10)
        return Transaction.SUCCEED

    def __str__(self):
        data = ''
        for job in self.jobs:
            if job.action == Transaction.PUSH:
                operator = job.src
            elif job.action == Transaction.REMOVE:
                operator = job.target
            else:
                raise Exception("unknown action")
            data += "%6s %s %s\n" % (job.action, job.status, operator)
        return data


def on_keyboard_interrupt(signum, frame):
    stop_flag.set()

stop_flag = Event()
signal.signal(signal.SIGINT, on_keyboard_interrupt)
