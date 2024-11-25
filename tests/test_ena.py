import logging
import shutil
import tempfile
import os
import time
from threading import Lock

from tests import SeqddTest
from seqdd.errors import DownloadError
from seqdd.register.sources.ena import ENA
from seqdd.utils.scheduler import CmdLineJob, FunctionJob


class TestEna(SeqddTest):


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


    def test_wait_my_turn(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        self.assertFalse(ena.mutex.locked())
        ena.wait_my_turn()
        self.assertTrue(ena.mutex.locked())
        ena.mutex.release()


    def test_jobs_from_accessions(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        acc = 'GCA_003543015.1'
        datadir = os.path.join(self._tmp_dir.name, 'data')
        os.mkdir(datadir)
        jobs = ena.jobs_from_accessions([acc], datadir)
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


    def test_move_and_clean_wo_md5(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        acc_dir = os.path.join(self._tmp_dir.name, 'acc_dir')
        out_dir = os.path.join(self._tmp_dir.name, 'out_dir')
        os.mkdir(acc_dir)
        os.mkdir(out_dir)
        genome_path = self.find_data('Genomes', 'AF268967.fa')
        arc_path = self.copy_file(genome_path, acc_dir, zip=True)
        ena.move_and_clean(acc_dir, out_dir)
        dest = os.path.join(out_dir, 'acc_dir', os.path.basename(genome_path)) +'.zip'
        self.assertTrue(os.path.exists(dest))
        self.assertFalse(os.path.exists(acc_dir))


    def test_move_and_clean_w_bad_md5(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        acc_dir = os.path.join(self._tmp_dir.name, 'acc_dir')
        out_dir = os.path.join(self._tmp_dir.name, 'out_dir')
        os.mkdir(acc_dir)
        os.mkdir(out_dir)
        genome_path = self.find_data('Genomes', 'AF268967.fa')
        arc_path = self.copy_file(genome_path, acc_dir, zip=True)
        md5 = {f"{os.path.basename(arc_path)}": self.md5sum(genome_path)}

        with self.catch_log(log_name='seqdd') as log:
            with self.assertRaises(DownloadError) as ctx:
                ena.move_and_clean(acc_dir, out_dir, md5s=md5)
            log_msg = log.get_value().rstrip()
        self.assertEqual(log_msg,
                         f'MD5 hash mismatch for file {os.path.basename(arc_path)} in accession {acc_dir}.\n'
                         'Accession files will not be downloaded.')


    def test_move_and_clean_w_good_md5(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        acc_dir = os.path.join(self._tmp_dir.name, 'acc_dir')
        out_dir = os.path.join(self._tmp_dir.name, 'out_dir')
        os.mkdir(acc_dir)
        os.mkdir(out_dir)
        genome_path = self.find_data('Genomes', 'AF268967.fa')
        arc_path = self.copy_file(genome_path, acc_dir, zip=True)
        md5 = {os.path.basename(arc_path): self.md5sum(arc_path)}
        ena.move_and_clean(acc_dir, out_dir, md5s=md5)
        dest = os.path.join(out_dir, 'acc_dir', os.path.basename(genome_path)) +'.zip'
        self.assertTrue(os.path.exists(dest))
        self.assertFalse(os.path.exists(acc_dir))


    def test_validate_accession(self):
        accessions = {
            'SRP000000009': 'Study',
            'PRJEZ000000000': 'Study',
            'SRS000006': 'Sample',
            'SAMN000000000': 'Sample',
            'GCA_004000535.1': 'Assembly',
            'ERA000000009': 'Submission',
            'ERX000000009': 'Experiment'}
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        for acc, exp_acc_type in accessions.items():
            with self.subTest(acc=acc):
                got_acc_type = ena.validate_accession(acc)
            self.assertEqual(got_acc_type, exp_acc_type)

        with self.catch_log() as log:
            acc = 'EDX123456789'
            got_acc_type = ena.validate_accession(acc)
            log_msg = log.get_value().rstrip()
        self.assertEqual(got_acc_type, 'Invalid')
        self.assertEqual(log_msg,
                         f'Invalid accession: {acc}')
