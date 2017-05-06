
import os
import logging

import oss2
from oss2.http import Session as Oss_Session

from . import utils, transaction


__all__ = ["Snapshot", "LocalSnapshot", "AliOssSnapshot"]

logger = logging.getLogger(__name__)


class Snapshot:
    def __init__(self, root):
        self.root = root
        self.file_ids = set()
        self._scan()
        logger.info("%s files in %s", len(self.file_ids), self.root)

    @staticmethod
    def get_instance(path):
        if path.find("alioss") == 0:
            if len(path) > 9:
                # alioss://endpoint/bucket
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
            for md5, path in only_in_self:
                data += "    %s %s\n" % (md5[0:6], path)

        if only_in_other:
            data += "only in %s ->\n" % snapshot.root
            for md5, path in only_in_other:
                data += "    %s %s\n" % (md5[0:6], path)

        return data

    def diff(self, snapshot):
        """diff two snapshots, get two list of file identity, with of each is
         sorted by file path.

        :return: (only_in_self, only_in_other)
        """
        s = self.file_ids.intersection(snapshot.file_ids)
        only_in_self = self._sort(self.file_ids.difference(s))
        only_in_other = self._sort(snapshot.file_ids.difference(s))
        return only_in_self, only_in_other

    def push_to(self, snapshot):
        raise NotImplementedError

    def _scan(self):
        raise NotImplementedError

    @staticmethod
    def _sort(id_set):
        return sorted(id_set, key=lambda x: x[1])

    def __str__(self):
        data = "%s -> %s files\n" % (self.root, len(self.file_ids))
        for md5, path in self._sort(self.file_ids):
            data += "    %s %s\n" % (md5[0:6], path)
        return data

    def __len__(self):
        return len(self.file_ids)

    def __getstate__(self):
        """Support pickle. If override by subcloss, this method will not be
        invoked automatically.
        """
        return {"root": self.root,
                "file_ids": self.file_ids}


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

                if os.path.isdir(sub_path):
                    dir_set.add((sub_path, sub_relative_path))
                else:
                    md5_sum = utils.get_md5(sub_path).upper()
                    self.file_ids.add((md5_sum, sub_relative_path))


class AliOssSnapshot(Snapshot):

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
                objs = [(o.etag, o.key)
                        for o in oss2.ObjectIterator(self.bucket, marker=marker,
                                                     max_keys=1000)]
            except Exception as e:
                logger.exception(e)
                raise utils.SnapshotError('scan AliOss bucket failed.')

            if not objs:
                break
            else:
                for etag, key in objs:
                    self.file_ids.add((etag.upper(), key))
                marker = objs[-1][1]

    def refresh_session(self):
        """release underlying session in oss2.

        It seems that there is not interface to release underlying socket
        resource in oss, which will cause warning message when using unittest
        This method is crude, but simple."""

        self.bucket.session.session.close()
        self.bucket.session = Oss_Session()

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
