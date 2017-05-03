
import os

import oss2

from . import utils, transaction


class Snapshot:
    def __init__(self, root):
        self.root = root
        self.file_ids = set()
        self._scan()

    def diff(self, snapshot):
        only_in_self, only_in_other = self._diff(snapshot)
        data = ''
        if only_in_self:
            data += "only in %s ->\n" % self.root
            for md5, path in only_in_self:
                data += "%s %s\n" % (md5[0:6], path)

        if only_in_other:
            data += "only in %s ->\n" % snapshot.root
            for md5, path in only_in_other:
                data += "%s %s\n" % (md5[0:6], path)

        return data

    def _diff(self, snapshot):
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
        data = self.root + "\n"
        for md5, path in self._sort(self.file_ids):
            data += "%s %s\n" % (md5[0:6], path)
        return data


class LocalSnapshot(Snapshot):

    def __init__(self, root_dir):
        if not os.path.isdir(root_dir):
            raise Exception("LocalSnapshot need directory.")
        root = os.path.abspath(os.path.realpath(root_dir))
        Snapshot.__init__(self, root)

    def push_to(self, snapshot):
        if isinstance(snapshot, AliOssSnapshot):
            return transaction.Local2AliOssTransaction()
        else:
            raise Exception("%s not support." % type(snapshot))

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

    def __init__(self, endpoint, bucket, access_key_id, access_key_secret):
        auth = oss2.Auth(access_key_id, access_key_secret)
        self.bucket = oss2.Bucket(auth, endpoint, bucket)
        root = "%s:%s" % (endpoint, bucket)
        Snapshot.__init__(self, root)

    def push_to(self, snapshot):
        raise NotImplementedError

    def _scan(self):
        marker = ""

        while True:
            objs = [(o.etag, o.key) for o in oss2.ObjectIterator(self.bucket, marker=marker, max_keys=1000)]
            if not objs:
                break
            else:
                for etag, key in objs:
                    self.file_ids.add((etag.upper(), key))
                marker = objs[-1][1]
