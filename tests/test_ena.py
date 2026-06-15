import logging
import tempfile
import os
import time
from threading import Lock
from unittest.mock import patch

from tests import SeqddTest

from seqdd.errors import DownloadError
from seqdd.register.sources.ena import ENA, move_and_clean
from seqdd.utils.scheduler import FunctionJob


# --- Mock helpers replacing the former curl subprocess calls ---

_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def _read_fixture(name):
    with open(os.path.join(_DATA_DIR, name), encoding='utf-8') as f:
        return f.read()


def fake_ena_http_get_text(url):
    """Stand-in for net.http_get_text dispatching on the ENA URL, backed by recorded fixtures."""
    if '/ena/browser/api/xml/' in url:
        acc = url.split('/xml/')[1].split('?')[0]
        if acc == 'ERR00001.BAD':
            raise DownloadError('xml endpoint unreachable')
        return _read_fixture(f'subprocess_get_ena_ftp_url_{acc}.out')
    if '/ena/portal/api/filereport?accession=' in url:
        acc = url.split('accession=')[1].split('&')[0]
        if acc == 'ERR00002.BAD':
            raise DownloadError('filereport endpoint unreachable')
        return _read_fixture(f'subprocess_get_ena_ftp_url_fastq_{acc}.out')
    raise AssertionError(f'unexpected URL {url}')


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
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        self.assertEqual(type(ena), ENA)
        self.assertEqual(ena.tmp_dir, self.seqdd_tmp_dir)
        self.assertEqual(ena.logger, self.logger)
        self.assertEqual(ena.last_query, 0)
        self.assertEqual(ena.min_delay, 0.35)
        self.assertTrue(isinstance(ena.mutex, type(Lock())))


    def test_src_delay_ready(self):
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        self.assertTrue(ena.source_delay_ready())
        ena.last_query = time.time()
        self.assertFalse(ena.source_delay_ready())


    def test_wait_my_turn(self):
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        self.assertFalse(ena.mutex.locked())
        ena.wait_my_turn()
        self.assertTrue(ena.mutex.locked())
        ena.mutex.release()


    def test_jobs_from_accessions(self):
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        acc = 'GCA_003543015.1'
        datadir = os.path.join(self._tmp_dir.name, 'data')
        os.mkdir(datadir)
        jobs = ena.jobs_from_accessions([acc], datadir)
        self.assertEqual(len(jobs), 2)
        download, clean = jobs
        self.assertEqual(download.name, f'ena_{acc}_{acc}_download')
        self.assertEqual(clean.name, f'ena_{acc}_{acc}_move')
        self.assertTrue(isinstance(download, FunctionJob))
        self.assertTrue(isinstance(clean, FunctionJob))
        self.assertEqual(download.parents, [])
        self.assertEqual(clean.parents, [download])


    def test_jobs_from_acc_data_exists(self):
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        acc = 'GCA_003543015.1'
        datadir = os.path.join(self._tmp_dir.name, 'data')
        os.mkdir(datadir)
        os.mkdir(self.seqdd_tmp_dir)
        open(os.path.join(datadir, acc), 'w').close()
        jobs = ena.jobs_from_accessions([acc], datadir)
        self.assertEqual(jobs, [])


    def test_jobs_from_acc_sample(self):
        def fake_get_ena_ftp_url(acc):
            return [("ftp.sra.ebi.ac.uk/vol1/fastq/SRR000/SRR000079/SRR000079.fastq.gz", '59b75f414522af095fd988aa69530e1e'),
                    ("ftp.sra.ebi.ac.uk/vol1/fastq/SRR288/SRR288080/SRR288080.fastq.gz", '3977ffc0b88f0dd8bbe2bd87aa8e66d8'),
                    ("ftp.sra.ebi.ac.uk/vol1/fastq/SRR000/SRR000078/SRR000078.fastq.gz", '0368c41da46aa510322b73f18d40452e')]

        ena = ENA(self.seqdd_tmp_dir, self.logger)
        acc = 'SRS000006'
        datadir = os.path.join(self._tmp_dir.name, 'data')
        os.mkdir(datadir)
        ori_get_ena_ftp_url = ena.get_ena_ftp_url
        ena.get_ena_ftp_url = fake_get_ena_ftp_url
        try:
            jobs = ena.jobs_from_accessions([acc], datadir)
        finally:
            ena.get_ena_ftp_url = ori_get_ena_ftp_url

        self.assertEqual(len(jobs), 4)
        self.assertEqual(jobs[0].name, f'ena_{acc}_SRR000079.fastq.gz')
        self.assertEqual(jobs[1].name, f'ena_{acc}_SRR288080.fastq.gz')
        self.assertEqual(jobs[2].name, f'ena_{acc}_SRR000078.fastq.gz')
        self.assertEqual(jobs[3].name, f'ena_{acc}_move')
        self.assertTrue(isinstance(jobs[0], FunctionJob))
        self.assertTrue(isinstance(jobs[1], FunctionJob))
        self.assertTrue(isinstance(jobs[2], FunctionJob))
        self.assertTrue(isinstance(jobs[3], FunctionJob))
        self.assertEqual(jobs[0].parents, [])
        self.assertEqual(jobs[1].parents, [])
        self.assertEqual(jobs[2].parents, [])
        self.assertEqual(jobs[3].parents, jobs[:3])


    def test_jobs_from_assembly(self):
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        acc = 'GCA_003543015.1'
        outdir = os.path.join(self._tmp_dir.name, 'outdir')
        job_name = 'nimportnaoik'
        jobs = ena.jobs_from_assembly(acc, self.seqdd_tmp_dir, outdir, job_name)
        self.assertEqual(len(jobs), 2)
        download, clean = jobs
        self.assertEqual(download.name, f'{job_name}_{acc}_download')
        self.assertEqual(clean.name, f'{job_name}_{acc}_move')
        self.assertTrue(isinstance(download, FunctionJob))
        self.assertTrue(isinstance(clean, FunctionJob))
        self.assertEqual(download.parents, [])
        self.assertEqual(clean.parents, [download])


    def test_move_and_clean_wo_md5(self):
        acc_dir = os.path.join(self._tmp_dir.name, 'acc_dir')
        out_dir = os.path.join(self._tmp_dir.name, 'out_dir')
        os.mkdir(acc_dir)
        os.mkdir(out_dir)
        genome_path = self.find_data('Genomes', 'AF268967.fa')
        self.copy_file(genome_path, acc_dir, zip=True)
        move_and_clean(acc_dir, out_dir)
        dest = os.path.join(out_dir, 'acc_dir', os.path.basename(genome_path)) +'.zip'
        self.assertTrue(os.path.exists(dest))
        self.assertFalse(os.path.exists(acc_dir))


    def test_move_and_clean_w_bad_md5(self):
        acc_dir = os.path.join(self._tmp_dir.name, 'acc_dir')
        out_dir = os.path.join(self._tmp_dir.name, 'out_dir')
        os.mkdir(acc_dir)
        os.mkdir(out_dir)
        genome_path = self.find_data('Genomes', 'AF268967.fa')
        arc_path = self.copy_file(genome_path, acc_dir, zip=True)
        md5 = {f"{os.path.basename(arc_path)}": self.md5sum(genome_path)}

        with self.catch_log(log_name='seqdd') as log:
            with self.assertRaises(DownloadError) as ctx:
                move_and_clean(acc_dir, out_dir, md5s=md5)
            log_msg = log.get_value().rstrip()

            exp_msg = (f'MD5 hash mismatch for file {os.path.basename(arc_path)} in accession {acc_dir}.\n'
                       f'Accession files will not be downloaded.')
            self.assertEqual(log_msg,
                             exp_msg)
        self.assertEqual(str(ctx.exception), exp_msg)


    def test_move_and_clean_w_good_md5(self):
        acc_dir = os.path.join(self._tmp_dir.name, 'acc_dir')
        out_dir = os.path.join(self._tmp_dir.name, 'out_dir')
        os.mkdir(acc_dir)
        os.mkdir(out_dir)
        genome_path = self.find_data('Genomes', 'AF268967.fa')
        arc_path = self.copy_file(genome_path, acc_dir, zip=True)
        md5 = {os.path.basename(arc_path): self.md5sum(arc_path)}
        move_and_clean(acc_dir, out_dir, md5s=md5)
        dest = os.path.join(out_dir, 'acc_dir', os.path.basename(genome_path)) +'.zip'
        self.assertTrue(os.path.exists(dest))
        self.assertFalse(os.path.exists(acc_dir))


    def test_filter_validate_accessions(self):
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        ena_valid_accessions_on_API_ori = ena.valid_accessions_on_API

        accs = ['GCA_003543015.1', 'GCA_00000000.BAD', 'GCA_00000000.99', 'SRS000006']
        ena.valid_accessions_on_API = lambda x : x
        try:
            with self.catch_log(log_name='seqdd'):
                # filter_validate_accessions does not log anythings
                # but it call validate_accession which log invalid (regexp) acc
                # do not test the log as it is tested with test_validate_accession
                valid_accs = ena.filter_valid(accs)
        finally:
            ena.valid_accessions_on_API = ena_valid_accessions_on_API_ori

        self.assertListEqual([accs[0], accs[-1]], valid_accs)


    def test_valid_accesions_on_API(self):
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        accs = ['GCA_003543015.1']
        body = _read_fixture(f'subprocess_resp_valid_acc_API_{accs[0]}.out')
        with patch('seqdd.utils.net.http_get_text', return_value=body):
            valid_accs = ena.valid_accessions_on_API(accs)
        self.assertEqual(valid_accs, accs)


    def test_valid_accesions_on_API_unk_acc(self):
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        accs = ['GCA_00000000.99']
        body = _read_fixture(f'subprocess_resp_valid_acc_API_{accs[0]}.out')
        query = f'https://www.ebi.ac.uk/ena/browser/api/xml/{accs[0]}?download=false&gzip=false&includeLinks=false'
        with patch('seqdd.utils.net.http_get_text', return_value=body):
            with self.catch_log(log_name='seqdd') as log:
                valid_accs = ena.valid_accessions_on_API(accs)
                log_msg = log.get_value().rstrip()
        self.assertEqual(valid_accs, [])
        expected_log = f"""Error querying ENA
Query: {query}
Answer: {body}
Accession(s) not found on ENA servers: {', '.join(accs)}"""
        self.assertEqual(log_msg, expected_log)


    def test_valid_accesions_on_API_bad_acc(self):
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        accs = ['GCA_00000000.BAD']
        body = _read_fixture(f'subprocess_resp_valid_acc_API_{accs[0]}.out')
        query = f'https://www.ebi.ac.uk/ena/browser/api/xml/{accs[0]}?download=false&gzip=false&includeLinks=false'
        with patch('seqdd.utils.net.http_get_text', return_value=body):
            with self.catch_log(log_name='seqdd') as log:
                valid_accs = ena.valid_accessions_on_API(accs)
                log_msg = log.get_value().rstrip()
        self.assertEqual(valid_accs, [])
        expected_log = f"""Error querying ENA
Query: {query}
Answer: {body}
Accession(s) not found on ENA servers: {', '.join(accs)}"""
        self.assertEqual(log_msg, expected_log)


    def test_valid_accesions_on_API_mix_acc(self):
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        accs = ['GCA_003543015.1', 'SRS000006']
        body = _read_fixture('subprocess_resp_valid_acc_API_mix_acc.out')
        query = f'https://www.ebi.ac.uk/ena/browser/api/xml/{",".join(accs)}?download=false&gzip=false&includeLinks=false'
        with patch('seqdd.utils.net.http_get_text', return_value=body):
            with self.catch_log(log_name='seqdd') as log:
                valid_accs = ena.valid_accessions_on_API(accs)
                log_msg = log.get_value().rstrip()
        self.assertEqual(valid_accs, [])
        expected_log = f"""Error querying ENA
Query: {query}
Answer: {body}
Accession(s) not found on ENA servers: {', '.join(sorted(accs))}"""
        self.assertEqual(log_msg, expected_log)


    def test_valid_accesions_on_API_download_failed(self):
        # The pure-Python network layer raises DownloadError when the request fails.
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        accs = ['GCA_003543015.1']
        err = DownloadError('connection refused')
        query = f'https://www.ebi.ac.uk/ena/browser/api/xml/{accs[0]}?download=false&gzip=false&includeLinks=false'
        with patch('seqdd.utils.net.http_get_text', side_effect=err):
            with self.catch_log(log_name='seqdd') as log:
                valid_accs = ena.valid_accessions_on_API(accs)
                log_msg = log.get_value().rstrip()
        self.assertEqual(valid_accs, [])
        expected_log = f"""Error querying ENA
Query: {query}
Answer: {err}
Accession(s) not found on ENA servers: {', '.join(sorted(accs))}"""
        self.assertEqual(log_msg, expected_log)


    def test_validate_accession(self):
        accessions = {
            'SRP000000009': 'Study',
            'PRJEZ000000000': 'Study',
            'SRS000006': 'Sample',
            'SAMN000000000': 'Sample',
            'GCA_004000535.1': 'Assembly',
            'ERA000000009': 'Submission',
            'ERX000000009': 'Experiment'}
        ena = ENA(self.seqdd_tmp_dir, self.logger)
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
                         f"Invalid accession: {acc}")


    @patch('seqdd.utils.net.http_get_text', side_effect=fake_ena_http_get_text)
    def test_get_ena_ftp_url_no_fastq(self, mocked):
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        acc = 'GCA_003543015.1'

        with self.catch_log(log_name='seqdd') as log:
            to_download = ena.get_ena_ftp_url(acc)
            log_msg = log.get_value().rstrip()
        self.assertListEqual(to_download, [])
        self.assertEqual(log_msg,
                  f'No fastq files found for accession {acc}')


    @patch('seqdd.utils.net.http_get_text', side_effect=fake_ena_http_get_text)
    def test_get_ena_ftp_url_fastq(self, mocked):
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        acc = 'ERR3258091'
        to_download = ena.get_ena_ftp_url(acc)
        self.assertEqual(len(to_download), 1)
        self.assertTupleEqual(to_download[0],
                              ('ftp.sra.ebi.ac.uk/vol1/fastq/ERR325/001/ERR3258091/ERR3258091.fastq.gz', 'da03ef37d3f6f03c2a527449f7e562fd')
                              )

    @patch('seqdd.utils.net.http_get_text', side_effect=fake_ena_http_get_text)
    def test_get_ena_ftp_url_download_failed(self, mocked):
        # The first request (XML browser API) fails.
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        acc = 'ERR00001.BAD'
        with self.catch_log(log_name='seqdd') as log:
            to_download = ena.get_ena_ftp_url(acc)
            log_msg = log.get_value().rstrip()
        self.assertListEqual(to_download, [])
        exp_msg = f"""Error querying ENA
Query: https://www.ebi.ac.uk/ena/browser/api/xml/{acc}?download=false&gzip=false&includeLinks=false
Answer: xml endpoint unreachable"""
        self.assertEqual(log_msg, exp_msg)


    @patch('seqdd.utils.net.http_get_text', side_effect=fake_ena_http_get_text)
    def test_get_ena_ftp_url_download_failed_2(self, mocked):
        # The second request (filereport) fails.
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        acc = 'ERR00002.BAD'
        with self.catch_log(log_name='seqdd') as log:
            to_download = ena.get_ena_ftp_url(acc)
            log_msg = log.get_value().rstrip()
        self.assertListEqual(to_download, [])
        fastq_url = (f'https://www.ebi.ac.uk/ena/portal/api/filereport?accession={acc}'
                     '&result=read_run&fields=run_accession,fastq_ftp,fastq_md5,fastq_bytes')
        exp_msg = f"""Error querying ENA
Query: {fastq_url}
Answer: filereport endpoint unreachable"""
        self.assertEqual(log_msg, exp_msg)


    @patch('seqdd.utils.net.http_get_text', side_effect=fake_ena_http_get_text)
    def test_get_ena_ftp_url_too_few_lines(self, mocked):
        # The filereport answer holds less than 2 lines.
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        acc = 'ERR00003.BAD'
        to_download = ena.get_ena_ftp_url(acc)
        self.assertListEqual(to_download, [])


    @patch('seqdd.utils.net.http_get_text', side_effect=fake_ena_http_get_text)
    def test_get_ena_ftp_url_no_fastq_columns(self, mocked):
        # The filereport header has neither fastq_ftp nor fastq_md5.
        ena = ENA(self.seqdd_tmp_dir, self.logger)
        acc = 'ERR00004.BAD'
        with self.catch_log(log_name='seqdd') as log:
            to_download = ena.get_ena_ftp_url(acc)
            log_msg = log.get_value().rstrip()
        self.assertListEqual(to_download, [])
        self.assertEqual(log_msg,
                         f'No fastq files found for accession {acc}')
