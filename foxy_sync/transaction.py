
import os
import sys
import pickle
import signal
import base64
import logging
from datetime import datetime

import oss2

from .utils import (Config, SnapshotError, TransactionError, JobError,
                    FoxyException)
from . import snapshot


__all__ = ["Transaction", "Local2AliOssTransaction"]

logger = logging.getLogger(__name__)


class _Job:

    # status
    READY = "ready"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELED = "canceled"

    # action
    PUSH = "push"
    REMOVE = "remove"

    def __init__(self, src, target, action, status=READY,
                 md5=None, mtime=None, info="", size=0):
        self.src = src
        self.target = target
        self.md5 = md5
        self.mtime = mtime
        self.action = action
        self.status = status
        self.info = info
        self.size = size

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class Transaction:

    def __init__(self, src_snapshot, target_snapshot):
        self.src_snapshot = src_snapshot
        self.target_snapshot = target_snapshot
        self.name = "%s_%s>>%s" % (datetime.now().strftime("%Y-%m-%d_%H:%M:%S"),
                                   src_snapshot.short_name,
                                   target_snapshot.short_name)
        self.jobs = None

    def start(self):

        self.get_jobs()

        finished_number = 0
        failed_number = 0
        canceled_number = 0
        ready_list = []

        for job in self.jobs:
            # some jobs may already finished, for transaction may be loaded
            # from file.
            if job.status == _Job.FINISHED:
                continue

            job.status = _Job.READY
            ready_list.append(job)

        if not ready_list:
            logger.info("no job found.")
            sys.exit(0)

        logging.info("%s jobs, start...", len(ready_list))
        time_stamp = datetime.now()
        size = 0

        try:
            for i, job in enumerate(ready_list):
                try:
                    self._do(job)
                except Exception as e:
                    failed_number += 1
                    job.status = _Job.FAILED
                    if not isinstance(e, JobError):
                        # unexpected exception
                        job.info = str(e)
                        logger.exception(e)
                else:
                    finished_number += 1
                    size += job.size
                    job.status = _Job.FINISHED

                tmp_ts = datetime.now()
                interval = (tmp_ts-time_stamp).seconds
                if interval > 60*30:
                    logger.info("average speed: %s KB/s, %s remained",
                                round(size/interval/1024, 2),
                                len(ready_list) - i - 1)
                    size = 0
                    time_stamp = tmp_ts

        except Exception as e:
            logger.exception(e)
        except KeyboardInterrupt:
            pass
        finally:
            # suppress KeyboardInterrupt for transaction dump
            signal.signal(signal.SIGINT, lambda signum, frame: None)

        for job in ready_list:
            if job.status not in (_Job.FINISHED, _Job.FAILED):
                job.status = _Job.CANCELED
                canceled_number += 1

        self.dump()
        logging.info("total: %s finished, %s failed, %s canceled",
                     finished_number, failed_number, canceled_number)
        sys.exit(0)

    def dump(self):
        with open(self.dump_path, "wb") as f:
            pickle.dump(self, f)
        logger.info("dump to %s", self.dump_path)

    @property
    def dump_path(self):
        return os.path.join(Config().cache_dir, self.name+".ts")

    @staticmethod
    def load(path):
        with open(path, "rb") as f:
            return pickle.load(f)

    def get_jobs(self):
        """diff snapshots and generate jobs. This method will let snapshot load
        file details. Do dump if any exception raised. This will let snapshot be
        able to load file detail from the break point."""

        if self.jobs is not None:
            return

        try:
            self.jobs = self._get_jobs()
        except Exception as e:
            logger.exception(e)
            self.dump()
            raise TransactionError('load jobs failed: %s' % self.dump_path)
        except KeyboardInterrupt:
            self.dump()
            raise TransactionError('canceled: %s' % self.dump_path)

    def _get_jobs(self):
        """WARNING: job.info should be initialized as str."""
        raise NotImplementedError

    def _do(self, job):
        raise NotImplementedError

    def __len__(self):
        return len(self.jobs)

    def __eq__(self, other):
        if isinstance(other, Transaction):
            return self.name == other.name and self.jobs == other.jobs
        else:
            return False


class Local2AliOssTransaction(Transaction):

    def _get_jobs(self):
        for s in (self.src_snapshot, self.target_snapshot):
            s.load_detail(md5=True)

        new_list, removed_list = self.src_snapshot.diff(self.target_snapshot)
        new_path = {f.path for f in new_list}
        src_root = self.src_snapshot.root
        target_prefix = self.target_snapshot.prefix
        jobs = []

        for file_id in new_list:
            src = os.path.join(src_root, file_id.path)
            size = os.stat(src).st_size
            jobs.append(_Job(src=src, target=target_prefix+file_id.path,
                             md5=file_id.md5, action=_Job.PUSH, size=size))

        for file_id in removed_list:
            if file_id.path not in new_path:
                jobs.append(_Job(src=None, target=target_prefix+file_id.path,
                                 md5=None, action=_Job.REMOVE))

        return jobs

    def _do(self, job):
        config = Config()
        if job.action == _Job.PUSH:
            encode_md5 = base64.b64encode(bytearray.fromhex(job.md5)
                                          ).decode()
            headers = {"Content-MD5": encode_md5,
                       snapshot.AliOssSnapshot.meta_md5: job.md5}
            try:
                oss2.resumable_upload(
                        self.target_snapshot.bucket, job.target, job.src,
                        headers=headers,
                        store=oss2.ResumableStore(root=config.cache_dir),
                        multipart_threshold=config.multipart_threshold,
                        part_size=config.multipart_threshold,
                        num_threads=config.num_threads)

            except oss2.exceptions.InvalidDigest:
                job.info = "md5 mismatch"
                raise JobError

        elif job.action == _Job.REMOVE:
            self.target_snapshot.bucket.delete_object(job.target)

    def __str__(self):
        data = ''
        info = {_Job.FINISHED: 0,
                _Job.FAILED: 0,
                _Job.READY: 0,
                _Job.CANCELED: 0}

        for job in self.jobs:
            if job.action == _Job.PUSH:
                operator = '%s -> %s' % (job.src, job.target)
            elif job.action == _Job.REMOVE:
                operator = job.target
            else:
                raise TransactionError("unknown action")

            info[job.status] += 1
            data += "%-6s %-8s %s %s\n" % (job.action, job.status, operator,
                                           job.info)

        for key in sorted(info.keys()):
            data += "%s: %s  " % (key, info[key])
        return data
