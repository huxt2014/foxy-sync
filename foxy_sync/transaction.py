
import os
import sys
import time
import pickle
import signal
import logging
import concurrent.futures as futures
from threading import Event
from datetime import datetime

from .utils import Config, SnapshotError, TransactionError, JobError


__all__ = ["Transaction", "Local2AliOssTransaction"]

logger = logging.getLogger(__name__)


class _Job:

    # status
    READY = "ready"
    FINISHED = "finished"
    FAILED = "failed"
    SKIPPED = "skipped"

    # action
    PUSH = "push"
    REMOVE = "remove"

    def __init__(self, src, target, md5, action, status=READY,
                 info=""):
        self.src = src
        self.target = target
        self.md5 = md5
        self.action = action
        self.status = status
        self.info = info

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class Transaction:

    def __init__(self, src_snapshot, target_snapshot):
        self.src_snapshot = src_snapshot
        self.target_snapshot = target_snapshot
        self.name = "%s_%s>>%s" % (datetime.now().strftime("%Y-%m-%d_%H:%M:%S"),
                                   src_snapshot.root.replace("/", "|"),
                                   target_snapshot.root.replace("/", "|"))
        self.jobs = self._get_jobs()
        self.futures = []
        self.executor = futures.ThreadPoolExecutor(Config().max_workers)

    def start(self):

        total_number = 0
        finished_number = 0
        should_stop = False

        for job in self.jobs:
            # some jobs may already finished, for transaction may be loaded
            # from file.
            if job.status == _Job.FINISHED:
                continue

            job.status = _Job.READY
            f = self.executor.submit(self._do_job, job)
            f.job = job
            self.futures.append(f)
            total_number += 1

        if total_number == 0:
            logger.info("no job found.")
            sys.exit(0)

        logging.info("%s jobs, start...", total_number)

        while True:
            stop_flag.wait(30)
            if stop_flag.is_set():
                logger.info("got cancel signal.")
                # there may be some jobs that can not be canceled,
                # we should wait for them in some where, or their
                # status may not be set.
                for f in self.futures:
                    f.cancel()
                should_stop = True

            tmp_futures = []
            failed_number = 0
            for f in self.futures:
                try:
                    if should_stop:
                        # transaction is canceled
                        wait_time = None
                        logger.info("wait for job finished...")
                    else:
                        # do not wait
                        wait_time = 0.0

                    result = f.result(timeout=wait_time)

                    if isinstance(result, BaseException):
                        # executor will catch BaseException
                        failed_number += 1
                        f.job.status = _Job.FAILED
                        f.job.info = "%s %s" % (f.job.info, result)
                        logger.exception(result)
                    else:
                        finished_number += 1
                        f.job.status = _Job.FINISHED

                except futures.TimeoutError:
                    # running or in queue
                    tmp_futures.append(f)
                except futures.CancelledError:
                    pass

            # if all job are finished or remained jobs are all failed
            if not tmp_futures or len(tmp_futures) == failed_number:
                should_stop = True

            if finished_number > 50 or should_stop:
                logger.info("%s jobs finished", finished_number)
                finished_number = 0

            if should_stop:
                break
            else:
                self.futures = tmp_futures

        self.executor.shutdown()
        self.dump()
        logger.info("stopped")
        sys.exit(0)

    def dump(self):
        with open(self.dump_path, "wb") as f:
            pickle.dump(self, f)
        logger.info("dump to %s", self.dump_path)

    @property
    def dump_path(self):
        return os.path.join(Config().dump_dir, self.name+".ts")

    @staticmethod
    def load(path):
        with open(path, "rb") as f:
            return pickle.load(f)

    def _get_jobs(self):
        """WARNING: job.info should be initialized as str."""
        raise NotImplementedError

    def _do_job(self, job):
        raise NotImplementedError

    def __len__(self):
        return len(self.jobs)

    def __eq__(self, other):
        if isinstance(other, Transaction):
            return self.name == other.name and self.jobs == other.jobs
        else:
            return False

    def __getstate__(self):
        return {"src_snapshot": self.src_snapshot,
                "target_snapshot": self.target_snapshot,
                "name": self.name,
                "jobs": self.jobs}

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.futures = []
        self.executor = futures.ThreadPoolExecutor(Config().max_workers)


class Local2AliOssTransaction(Transaction):

    def _get_jobs(self):
        new_list, removed_list = self.src_snapshot.diff(self.target_snapshot)
        src_root = self.src_snapshot.root
        jobs = []

        for raw, action in ((new_list, _Job.PUSH),
                            (removed_list, _Job.REMOVE)):
            jobs += [_Job(src=os.path.join(src_root, file_id[1]),
                          target=file_id[1],
                          md5=file_id[0],
                          action=action)
                     for file_id in raw]
        return jobs

    def _do_job(self, job):
        time.sleep(10)

    def __str__(self):
        data = ''
        for job in self.jobs:
            if job.action == _Job.PUSH:
                operator = job.src
            elif job.action == _Job.REMOVE:
                operator = job.target
            else:
                raise TransactionError("unknown action")
            data += "%-6s %-8s %s\n" % (job.action, job.status, operator)
        return data


def on_keyboard_interrupt(signum, frame):
    stop_flag.set()

stop_flag = Event()
signal.signal(signal.SIGINT, on_keyboard_interrupt)
