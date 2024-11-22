import logging
import tempfile
import os
import time
from threading import Lock

from tests import SeqddTest
from seqdd.register.sources.ena import ENA
from seqdd.utils.scheduler import CmdLineJob, FunctionJob


class TestSource(SeqddTest):


    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')


    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-')
        self.seqdd_tmp_dir = os.path.join(self._tmp_dir.name, 'tmp')
        self.bin_dir = os.path.join(self._tmp_dir.name, 'bin')


    def tearDown(self):
        self._tmp_dir.cleanup()


    def test_ena(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        self.assertEqual(type(ena), ENA)
        self.assertEqual(ena.tmp_dir, self.seqdd_tmp_dir)
        self.assertEqual(ena.bin_dir, self.bin_dir)
        self.assertEqual(ena.logger, self.logger)
        self.assertEqual(ena.last_query, 0)
        self.assertEqual(ena.min_delay, 0.35)
        self.assertTrue(isinstance(ena.mutex, type(Lock())))


    def test_is_ready(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        self.assertTrue(ena.is_ready())


    def test_src_delay_ready(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        self.assertTrue(ena.src_delay_ready())
        ena.last_query = time.time()
        self.assertFalse(ena.src_delay_ready())


    def test_wait_may_turn(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        self.assertFalse(ena.mutex.locked())
        ena.wait_my_turn()
        self.assertTrue(ena.mutex.locked())
        ena.mutex.release()


    def test_jobs_from_accessions(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        acc = 'GCA_003543015.1'
        jobs = ena.jobs_from_accessions([acc], 'data')
        self.assertEqual(len(jobs), 3)
        curl, gzip, clean = jobs
        self.assertEqual(curl.name, f'ena_{acc}_{acc}_download')
        self.assertEqual(gzip.name, f'ena_{acc}_{acc}_gzip')
        self.assertEqual(clean.name, f'ena_{acc}_{acc}_move')
        self.assertTrue(isinstance(curl, CmdLineJob))
        self.assertTrue(isinstance(gzip, CmdLineJob))
        self.assertTrue(isinstance(clean, FunctionJob))
        self.assertEqual(curl.parents, [])
        self.assertEqual(gzip.parents, [curl])
        self.assertEqual(clean.parents, [gzip])
