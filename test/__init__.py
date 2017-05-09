
import os
import shutil
import unittest
import tempfile

from foxy_sync.snapshot import *
from foxy_sync.transaction import Transaction
from foxy_sync import utils


class CaseAlioss(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config = utils.Config()
        cls.alioss_snapshot = AliOssSnapshot(config.end_point,
                                             config.test_bucket)
        cls.alioss_snapshot.load_detail(md5=True)
        cls.alioss_snapshot.refresh_session()

    def test_print(self):
        print()
        print(self.alioss_snapshot)


class CaseLocal(unittest.TestCase):

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
        snapshot = Snapshot.get_instance(self.root)

        # _scan
        assert len(snapshot.files) == len(self.file_set)

        # load_detail
        snapshot.load_detail(md5=True, mtime=True)
        self.assertRaises(utils.SnapshotError, snapshot.load_detail,
                          md5=True, mtime=True)
        for f_id in snapshot.frozen_files:
            assert f_id.md5.lower() == self.md5, "%s %s" % (f_id.md5, self.md5)
            assert f_id.path in self.file_set, (f_id.path, self.file_set)

        # __len__
        assert len(snapshot) == len(self.file_set)

        # print
        print()
        print(snapshot)


class CaseTrans(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config = utils.Config()
        cls.local_snapshot = LocalSnapshot(config.test_local)
        cls.alioss_snapshot = AliOssSnapshot(config.end_point,
                                             config.test_bucket)
        for s in (cls.local_snapshot, cls.alioss_snapshot):
            s.load_detail(md5=True)
        cls.alioss_snapshot.refresh_session()

    def test_print(self):
        print()
        print(self.local_snapshot.diff_str(self.alioss_snapshot))

        transaction = self.local_snapshot.push_to(self.alioss_snapshot)
        transaction.get_jobs()
        print(transaction)

    def test_base(self):
        transaction1 = self.local_snapshot.push_to(self.alioss_snapshot)
        transaction2 = self.local_snapshot.push_to(self.alioss_snapshot)

        for t in (transaction1, transaction2):
            t.get_jobs()

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
