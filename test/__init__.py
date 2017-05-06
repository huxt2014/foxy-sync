
import os
import shutil
import unittest
import tempfile

from foxy_sync.snapshot import *
from foxy_sync.transaction import Transaction
from foxy_sync.utils import Config


class CasePrint(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.local_snapshot, cls.alioss_snapshot = load_test_snapshot()
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


class CaseLocalSnapshot(unittest.TestCase):

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

    def test_base(self):
        snapshot = LocalSnapshot(self.root)

        # _scan
        assert len(snapshot.file_ids) == len(self.file_set)
        for md5, path in snapshot.file_ids:
            assert md5.lower() == self.md5, "%s %s" % (md5, self.md5)
            assert path in self.file_set, (path, self.file_set)

        # __len__
        assert len(snapshot) == len(self.file_set)


class CaseTrans(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.local_snapshot, cls.alioss_snapshot = load_test_snapshot()
        cls.alioss_snapshot.refresh_session()

    def test_base(self):
        transaction1 = self.local_snapshot.push_to(self.alioss_snapshot)
        transaction2 = self.local_snapshot.push_to(self.alioss_snapshot)

        # __eq__
        self.assertFalse(transaction1 is transaction2)
        self.assertTrue(transaction1 == transaction2)
        self.assertFalse(transaction1 == set())

        self.assertTrue(len(transaction1) > 1)
        transaction1.jobs[0], transaction1.jobs[1] = (
            transaction1.jobs[1], transaction1.jobs[0])
        self.assertFalse(transaction1 == transaction2)

    def test_job(self):
        transaction = self.local_snapshot.push_to(self.alioss_snapshot)
        with self.assertRaises(SystemExit) as cm:
            transaction.start()
        self.assertEqual(cm.exception.args[0], 0)

        # pickle
        ts = Transaction.load(transaction.dump_path)
        self.assertTrue(transaction == ts)


def load_test_snapshot():
    config = Config()
    assert (config.test_local and config.end_point and config.test_bucket
            and config.access_key_id and config.access_key_secret)

    local_snapshot = LocalSnapshot(config.test_local)
    alioss_snapshot = AliOssSnapshot(config.end_point, config.test_bucket)

    return local_snapshot, alioss_snapshot
