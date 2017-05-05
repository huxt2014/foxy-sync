
import os
import shutil
import unittest
import tempfile

from foxy_sync.snapshot import *
from foxy_sync.utils import Config


class TestL2A(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config = Config()
        assert (config.test_local and config.end_point and config.test_bucket
                and config.access_key_id and config.access_key_secret)

        cls.local_snapshot = LocalSnapshot(config.test_local)
        cls.alioss_snapshot = AliOssSnapshot(
                                config.end_point, config.test_bucket,
                                config.access_key_id, config.access_key_secret)
        cls.alioss_snapshot.refresh_session()

    def test_print_snapshot(self):
        print()
        print(self.local_snapshot)
        print(self.alioss_snapshot)

    def test_print_dff(self):
        print()
        print(self.local_snapshot.diff_str(self.alioss_snapshot))

    def test_print_transaction(self):
        transaction = self.local_snapshot.push_to(self.alioss_snapshot)
        print()
        print(transaction)

    def test_job(self):
        transaction = self.local_snapshot.push_to(self.alioss_snapshot)
        with self.assertRaises(SystemExit) as cm:
            transaction.start()
        self.assertEqual(cm.exception.args[0], 0)


class TestLocalSnapshot(unittest.TestCase):

    root = None
    content = b"hello world"
    md5 = "5eb63bbbe01eeed093cb22bb8f5acdc3"

    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp()
        cls.file_set = set()
        dir1 = tempfile.mkdtemp(dir=cls.root)
        dir2 = tempfile.mkdtemp(dir=dir1)
        dir3 = tempfile.mkdtemp(dir=dir1)

        for d in (cls.root, dir1, dir2):
            fd, path = tempfile.mkstemp(dir=d)
            cls.file_set.add(path.replace(cls.root+"/", ""))
            os.write(fd, cls.content)
            os.close(fd)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.root)

    def test_tmp(self):
        snapshot = LocalSnapshot(self.root)
        assert len(snapshot.file_ids) == len(self.file_set)
        for md5, path in snapshot.file_ids:
            assert md5.lower() == self.md5, "%s %s" % (md5, self.md5)
            assert path in self.file_set, (path, self.file_set)
