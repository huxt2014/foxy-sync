
import os
import fnmatch
import logging
from datetime import datetime

import oss2
from oss2.http import Session as OssSession

from . import utils, transaction


__all__ = ["FileIdentity", "Snapshot", "LocalSnapshot", "AliOssSnapshot"]

logger = logging.getLogger(__name__)


class FileIdentity:

    def __init__(self, path, md5=None, mtime=None):
        self.path = path
        self.md5 = md5
        self.mtime = mtime

    def __str__(self):
        data = ""

        if self.md5:
            data += self.md5[0:6] + ' '
        if self.mtime:
            data += datetime.fromtimestamp(self.mtime
                                           ).strftime('%Y-%m-%d %H:%M:%S') + ' '

        data += self.path
        return data

    def __eq__(self, other):
        if isinstance(other, FileIdentity):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __hash__(self):
        """support set operation"""
        return hash((self.path, self.md5, self.mtime))


class Snapshot:
    def __init__(self, root):
        self.root = root
        self.files = []
        self._frozen_files = set()
        self.load_completed = False
        self._scan()
        logger.info("%s files in %s", len(self.files), self.root)

    @staticmethod
    def get_instance(path):
        """
        :param path: alioss[://endpoint/bucket] or /local/directory
        :return: 
        """
        if path.find("alioss") == 0:
            if len(path) > 9:
                segs = path[9:].split("/")
                if len(segs) != 2:
                    raise utils.SnapshotError('invalid AliOss path: %s' % path)
                else:
                    return AliOssSnapshot(segs[0], segs[1])
            else:
                config = utils.Config()
                return AliOssSnapshot(config.end_point, config.bucket)
        else:
            if os.path.isdir(path):
                return LocalSnapshot(path)
            else:
                raise utils.SnapshotError("invalid local directory %s" % path)

    def diff_str(self, snapshot):
        only_in_self, only_in_other = self.diff(snapshot)
        data = ''
        if only_in_self:
            data += "only in %s ->\n" % self.root
            for f_id in only_in_self:
                data += "    %s\n" % f_id

        if only_in_other:
            data += "only in %s ->\n" % snapshot.root
            for f_id in only_in_other:
                data += "    %s\n" % f_id

        return data

    def diff(self, snapshot):
        """diff two snapshots, get two list of file identity, with of each is
         sorted by file path.

        :return: (only_in_self, only_in_other)
        """
        s = self.frozen_files.intersection(snapshot.frozen_files)
        only_in_self = self._sort(self.frozen_files.difference(s))
        only_in_other = self._sort(snapshot.frozen_files.difference(s))
        return only_in_self, only_in_other

    @property
    def frozen_files(self):
        if not self.load_completed:
            raise utils.SnapshotError("snapshot loading unfinished, call  "
                                      "load_detail() first.")
        return self._frozen_files

    def push_to(self, snapshot):
        raise NotImplementedError

    def _scan(self):
        raise NotImplementedError

    def load_detail(self, md5=False, mtime=False):
        """get all file's md5 and mtime, Once called successfully, can not be
        called any more."""
        if self.load_completed:
            return
        self._load_detail(md5=md5, mtime=mtime)
        self.load_completed = True

    @staticmethod
    def should_skip(path, directory=False, key=False):
        if directory:
            target_dir = path
        elif key:
            target_dir = os.path.dirname(path)
        else:
            raise utils.SnapshotError("unknown path type")

        config = utils.Config()
        for p in config.skip_dir:
            if fnmatch.fnmatch(target_dir, p):
                return True

        return False

    def _load_detail(self, md5=False, mtime=False):
        raise NotImplementedError

    @staticmethod
    def _sort(id_set):
        return sorted(id_set, key=lambda x: x.path)

    def __str__(self):
        data = "%s -> %s files\n" % (self.root, len(self.frozen_files))
        for f_id in self._sort(self.frozen_files):
            data += "    %s\n" % f_id
        return data

    def __len__(self):
        return len(self.files)

    def __getstate__(self):
        """Support pickle. Make subclass getting state of super class easier..
        """
        return {"root": self.root,
                "files": self.files,
                "_frozen_files": self._frozen_files,
                "load_completed": self.load_completed}


class LocalSnapshot(Snapshot):

    def __init__(self, root_dir):
        if not os.path.isdir(root_dir):
            raise utils.SnapshotError("LocalSnapshot need directory.")
        root = os.path.abspath(os.path.realpath(root_dir))
        Snapshot.__init__(self, root)

    def push_to(self, snapshot):
        """
        :return: transaction
        """
        if isinstance(snapshot, AliOssSnapshot):
            return transaction.Local2AliOssTransaction(self, snapshot)
        else:
            raise utils.SnapshotError("snapshot type not support: %s " %
                                      type(snapshot))

    @property
    def short_name(self):
        return os.path.basename(self.root)

    def _scan(self):
        """WARNING: not support cyclic path and link."""

        dir_set = {(self.root, "")}

        while True:
            if not dir_set:
                break

            path, relative_path = dir_set.pop()

            for file in os.listdir(path):
                sub_path = os.path.join(path, file)
                sub_relative_path = os.path.join(relative_path, file)

                if (os.path.isdir(sub_path)
                   and not self.should_skip(sub_relative_path, directory=True)):
                    dir_set.add((sub_path, sub_relative_path))
                else:
                    self.files.append(FileIdentity(sub_relative_path))

    def _load_detail(self, md5=False, mtime=False):

        for f_id in self.files:
            path = os.path.join(self.root, f_id.path)

            if md5 and f_id.md5 is None:
                f_id.md5 = utils.get_md5(path).upper()
            if mtime and f_id.mtime is None:
                f_id.mtime = os.stat(path).st_mtime

            self._frozen_files.add(f_id)


class AliOssSnapshot(Snapshot):

    meta_md5 = "x-oss-meta-md5"

    def __init__(self, endpoint, bucket):
        self._endpoint = endpoint
        self._bucket = bucket
        Snapshot.__init__(self, "%s:%s" % (endpoint, bucket))

    def push_to(self, snapshot):
        raise NotImplementedError

    def _scan(self):
        marker = ""

        while True:
            try:
                objs = [o for o in
                        oss2.ObjectIterator(self.bucket, marker=marker,
                                            max_keys=1000)]
            except Exception as e:
                logger.exception(e)
                raise utils.SnapshotError('scan AliOss bucket failed.')

            if not objs:
                break
            else:
                for o in objs:
                    if not self.should_skip(o.key, key=True):
                        self.files.append(FileIdentity(o.key))
                marker = objs[-1].key

    def refresh_session(self):
        """release underlying session in oss2.

        It seems that there is not interface to release underlying socket
        resource in oss, which will cause warning message when using unittest
        This method is crude, but simple."""

        self.bucket.session.session.close()
        self.bucket.session = OssSession()

    def _load_detail(self, md5=False, mtime=False):
        if mtime:
            raise utils.SnapshotError("AliOssSnapshot not support file mtime.")

        for f_id in self.files:
            if md5 and f_id.md5 is None:
                meta = self.bucket.head_object(f_id.path)
                f_id.md5 = meta.headers.get(self.meta_md5, "").upper()
            self._frozen_files.add(f_id)

    @property
    def short_name(self):
        return self._bucket

    @utils.lazy_property
    def bucket(self):
        config = utils.Config()
        if not config.access_key_id or not config.access_key_secret:
            raise utils.SnapshotError("access_key_id or access_key_secret "
                                      "missing")

        auth = oss2.Auth(config.access_key_id, config.access_key_secret)
        return oss2.Bucket(auth, self._endpoint, self._bucket)

    def __getstate__(self):
        state = Snapshot.__getstate__(self)
        state.update({"_endpoint": self._endpoint,
                      "_bucket": self._bucket})
        return state
