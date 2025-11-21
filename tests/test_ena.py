import logging
import tempfile
import os
import time
from threading import Lock
from subprocess import CompletedProcess
from unittest.mock import Mock, patch

from tests import SeqddTest

from seqdd.errors import DownloadError
import seqdd.register.sources.ena
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


    def test_src_delay_ready(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        self.assertTrue(ena.source_delay_ready())
        ena.last_query = time.time()
        self.assertFalse(ena.source_delay_ready())


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


    def test_jobs_from_acc_data_exists(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
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

        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
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
        self.assertTrue(isinstance(jobs[0], CmdLineJob))
        self.assertTrue(isinstance(jobs[1], CmdLineJob))
        self.assertTrue(isinstance(jobs[2], CmdLineJob))
        self.assertTrue(isinstance(jobs[3], FunctionJob))
        self.assertEqual(jobs[0].parents, [])
        self.assertEqual(jobs[1].parents, [])
        self.assertEqual(jobs[2].parents, [])
        self.assertEqual(jobs[3].parents, jobs[:3])


    def test_jobs_from_assembly(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        acc = 'GCA_003543015.1'
        outdir = os.path.join(self._tmp_dir.name, 'outdir')
        job_name = 'nimportnaoik'
        jobs = ena.jobs_from_assembly(acc, self.seqdd_tmp_dir, outdir, job_name)
        self.assertEqual(len(jobs), 3)
        curl, gzip, clean = jobs
        self.assertEqual(curl.name, f'{job_name}_{acc}_download')
        self.assertEqual(gzip.name, f'{job_name}_{acc}_gzip')
        self.assertEqual(clean.name, f'{job_name}_{acc}_move')
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
        self.copy_file(genome_path, acc_dir, zip=True)
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

            exp_msg = (f'MD5 hash mismatch for file {os.path.basename(arc_path)} in accession {acc_dir}.\n'
                       f'Accession files will not be downloaded.')
            self.assertEqual(log_msg,
                             exp_msg)
        self.assertEqual(str(ctx.exception), exp_msg)


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


    def test_filter_validate_accessions(self):
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
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
        # valid_accesions_on_API use subprocess to spawn a curl subprocess
        # we mock subprocess run to mimic the response of ENA
        seqdd.register.sources.ena.subprocess = Mock()
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        accs = ['GCA_003543015.1']
        fake_curl_response = CompletedProcess(args=['fakecurl', 'https://www.ebi.ac.uk/ena/browser/api/xml/GCA_003543015.1?download=false&gzip=false&includeLinks=false'],
                                    returncode=0)
        with open(self.find_data(f'subprocess_resp_valid_acc_API_{accs[0]}.err'), 'rb') as f:
            fake_curl_response.stderr = f.read()
        with open(self.find_data(f'subprocess_resp_valid_acc_API_{accs[0]}.out'), 'rb') as f:
            fake_curl_response.stdout = f.read()

        seqdd.register.sources.ena.subprocess.run.return_value = fake_curl_response
        valid_accs = ena.valid_accessions_on_API(accs)
        self.assertEqual(valid_accs, accs)


    def test_valid_accesions_on_API_unk_acc(self):
        # valid_accesions_on_API use subprocess to spawn a curl subprocess
        # we mock subprocess run to mimic the response of ENA
        seqdd.register.sources.ena.subprocess = Mock()
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        accs = ['GCA_00000000.99']
        fake_curl_response = CompletedProcess(
            args=['fakecurl',
                  f'https://www.ebi.ac.uk/ena/browser/api/xml/{accs[0]}?download=false&gzip=false&includeLinks=false'],
            returncode=0)
        with open(self.find_data(f'subprocess_resp_valid_acc_API_{accs[0]}.err'), 'rb') as f:
            fake_curl_response.stderr = f.read()
        with open(self.find_data(f'subprocess_resp_valid_acc_API_{accs[0]}.out'), 'rb') as f:
            fake_curl_response.stdout = f.read()

        seqdd.register.sources.ena.subprocess.run.return_value = fake_curl_response
        with self.catch_log(log_name='seqdd') as log:
            valid_accs = ena.valid_accessions_on_API(accs)
            log_msg = log.get_value().rstrip()
        self.assertEqual(valid_accs, [])
        expected_log = f"""Error querying ENA
Query: {fake_curl_response.args[1]}
Answer: {fake_curl_response.stdout.decode()}
Accession(s) not found on ENA servers: {', '.join(accs)}"""

        self.assertEqual(log_msg, expected_log)


    def test_valid_accesions_on_API_bad_acc(self):
        # valid_accesions_on_API use subprocess to spawn a curl subprocess
        # we mock subprocess run to mimic the response of ENA
        seqdd.register.sources.ena.subprocess = Mock()
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        accs = ['GCA_00000000.BAD']
        fake_curl_response = CompletedProcess(
            args=['fakecurl', 'https://www.ebi.ac.uk/ena/browser/api/xml/GCA_00000000.BAD?download=false&gzip=false&includeLinks=false'],
            returncode=0)
        with open(self.find_data(f'subprocess_resp_valid_acc_API_{accs[0]}.err'), 'rb') as f:
            fake_curl_response.stderr = f.read()
        with open(self.find_data(f'subprocess_resp_valid_acc_API_{accs[0]}.out'), 'rb') as f:
            fake_curl_response.stdout = f.read()

        seqdd.register.sources.ena.subprocess.run.return_value = fake_curl_response
        with self.catch_log(log_name='seqdd') as log:
            valid_accs = ena.valid_accessions_on_API(accs)
            log_msg = log.get_value().rstrip()
        self.assertEqual(valid_accs, [])
        expected_log = f"""Error querying ENA
Query: {fake_curl_response.args[1]}
Answer: {fake_curl_response.stdout.decode()}
Accession(s) not found on ENA servers: {', '.join(accs)}"""

        self.assertEqual(log_msg, expected_log)


    def test_valid_accesions_on_API_mix_acc(self):
        # valid_accesions_on_API use subprocess to spawn a curl subprocess
        # we mock subprocess run to mimic the response of ENA
        seqdd.register.sources.ena.subprocess = Mock()
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        accs = ['GCA_003543015.1', 'SRS000006']
        fake_curl_response = CompletedProcess(
            args=['fakecurl',
                  'https://www.ebi.ac.uk/ena/browser/api/xml/GCA_003543015.1,SRS000006?download=false&gzip=false&includeLinks=false'],
            returncode=0)
        with open(self.find_data('subprocess_resp_valid_acc_API_mix_acc.err'), 'rb') as f:
            fake_curl_response.stderr = f.read()
        with open(self.find_data('subprocess_resp_valid_acc_API_mix_acc.out'), 'rb') as f:
            fake_curl_response.stdout = f.read()

        seqdd.register.sources.ena.subprocess.run.return_value = fake_curl_response
        with self.catch_log(log_name='seqdd') as log:
            valid_accs = ena.valid_accessions_on_API(accs)
            log_msg = log.get_value().rstrip()
        self.assertEqual(valid_accs, [])
        expected_log = f"""Error querying ENA
Query: {fake_curl_response.args[1]}
Answer: {fake_curl_response.stdout.decode()}
Accession(s) not found on ENA servers: {', '.join(sorted(accs))}"""
        self.assertEqual(log_msg, expected_log)


    def test_valid_accesions_on_API_curl_failed(self):
        # valid_accesions_on_API use subprocess to spawn a curl subprocess
        # we mock subprocess run to mimic the response of ENA
        seqdd.register.sources.ena.subprocess = Mock()
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        accs = ['GCA_003543015.1']
        fake_curl_response = CompletedProcess(args=['curl', 'https://no_where_fake_url.org'],
                                              returncode=6)
        with open(self.find_data('subprocess_resp_valid_acc_API_curl_failed.err'), 'rb') as f:
            fake_curl_response.stderr = f.read()
        with open(self.find_data('subprocess_resp_valid_acc_API_curl_failed.out'), 'rb') as f:
            fake_curl_response.stdout = f.read()

        seqdd.register.sources.ena.subprocess.run.return_value = fake_curl_response
        with self.catch_log(log_name='seqdd') as log:
            valid_accs = ena.valid_accessions_on_API(accs)
            log_msg = log.get_value().rstrip()
        self.assertEqual(valid_accs, [])
        expected_log = f"""Error querying ENA
Query: {f"https://www.ebi.ac.uk/ena/browser/api/xml/{'GCA_003543015.1'}?download=false&gzip=false&includeLinks=false"}
Answer: {fake_curl_response.stderr.decode()}
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
                         f"Invalid accession: {acc}")


    def fake_ena_ftp_curl(args, stdout=None, stderr=None):
        cmde, url = args
        class MockResponse:

            def __init__(self, out, err, rcode):
                self.returncode = rcode
                self.stderr = err
                self.stdout = out

            def __enter__(self):
                return self

            def __exit__(self, type, value, traceback):
                return False

        if url.startswith('https://www.ebi.ac.uk/ena/browser/api/xml/'):
            acc = url.lstrip('https://www.ebi.ac.uk/ena/browser/api/xml/').rstrip('?download=false&gzip=false&includeLinks=false')
            if acc == 'ERR00001.BAD':
                returncode = 12
            else:
                returncode = 0

            with open(os.path.join(os.path.dirname(__file__), 'data',f'subprocess_get_ena_ftp_url_{acc}.err'), 'rb') as f:
                err = f.read()
            with open(os.path.join(os.path.dirname(__file__), 'data', f'subprocess_get_ena_ftp_url_{acc}.out'), 'rb') as f:
                out = f.read()
            return MockResponse(out, err, returncode)

        elif url.startswith('https://www.ebi.ac.uk/ena/portal/api/filereport?accession='):
            acc = url.lstrip('https://www.ebi.ac.uk/ena/portal/api/filereport?accession=').rstrip('&result=read_run&fields=run_accession,fastq_ftp,fastq_md5,fastq_bytes')
            if acc == 'ERR00002.BAD':
                returncode = 22
            else:
                returncode = 0

            with open(os.path.join(os.path.dirname(__file__), 'data', f'subprocess_get_ena_ftp_url_fastq_{acc}.err'), 'rb') as f:
                err = f.read()
            with open(os.path.join(os.path.dirname(__file__), 'data', f'subprocess_get_ena_ftp_url_fastq_{acc}.out'), 'rb') as f:
                out = f.read()
            return MockResponse(out, err, returncode)



    @patch('subprocess.run', side_effect=fake_ena_ftp_curl)
    def test_get_ena_ftp_url_no_fastq(self, mocked_run):
        # get_ena_ftp_url use subprocess to spawn a curl subprocess
        # we mock subprocess run to mimic the response of ENA
        # seqdd.register.data_sources.ena.subprocess = Mock()
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        acc = 'GCA_003543015.1'

        with self.catch_log(log_name='seqdd') as log:
            to_download = ena.get_ena_ftp_url(acc)
            log_msg = log.get_value().rstrip()
        self.assertListEqual(to_download, [])
        self.assertEqual(log_msg,
                  f'No fastq files found for accession {acc}')


    @patch('subprocess.run', side_effect=fake_ena_ftp_curl)
    def test_get_ena_ftp_url_fastq(self, mocked_run):
        # get_ena_ftp_url use subprocess to spawn a curl subprocess
        # we mock subprocess run to mimic the response of ENA
        # when fastq are available 2 calls to subprocess are performed
        # we cannot use a MagicMock
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        acc = 'ERR3258091'
        to_download = ena.get_ena_ftp_url(acc)
        self.assertEqual(len(to_download), 1)
        self.assertTupleEqual(to_download[0],
                              ('ftp.sra.ebi.ac.uk/vol1/fastq/ERR325/001/ERR3258091/ERR3258091.fastq.gz', 'da03ef37d3f6f03c2a527449f7e562fd')
                              )

    @patch('subprocess.run', side_effect=fake_ena_ftp_curl)
    def test_get_ena_ftp_url_curl_failed(self, mocked_run):
        # get_ena_ftp_url use subprocess to spawn a curl subprocess
        # we mock subprocess run to mimic the response of ENA
        # when fastq are available 2 calls to subprocess are performed
        # we cannot use a MagicMock

        # the first curl call return with non zero value
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        acc = 'ERR00001.BAD'
        with self.catch_log(log_name='seqdd') as log:
            to_download = ena.get_ena_ftp_url(acc)
            log_msg = log.get_value().rstrip()
        self.assertListEqual(to_download, [])
        with open(self.find_data(f'subprocess_get_ena_ftp_url_{acc}.err'), 'rb') as f:
                    err = f.read()
        exp_msg = f"""Error querying ENA
Query: https://www.ebi.ac.uk/ena/browser/api/xml/{acc}?download=false&gzip=false&includeLinks=false
Answer: {err.decode().rstrip()}"""
        self.assertEqual(log_msg, exp_msg)


    @patch('subprocess.run', side_effect=fake_ena_ftp_curl)
    def test_get_ena_ftp_url_curl_failed_2(self, mocked_run):
        # get_ena_ftp_url use subprocess to spawn a curl subprocess
        # we mock subprocess run to mimic the response of ENA
        # when fastq are available 2 calls to subprocess are performed
        # we cannot use a MagicMock

        # the second curl call return with non zero value
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        acc = 'ERR00002.BAD'
        with self.catch_log(log_name='seqdd') as log:
            to_download = ena.get_ena_ftp_url(acc)
            log_msg = log.get_value().rstrip()
        self.assertListEqual(to_download, [])
        with open(self.find_data(f'subprocess_get_ena_ftp_url_fastq_{acc}.err'), 'rb') as f:
                    err = f.read()
        exp_msg = f"""Error querying ENA
Query: https://www.ebi.ac.uk/ena/portal/api/filereport?accession={acc}&result=read_run&fields=run_accession,fastq_ftp,fastq_md5,fastq_bytes
Answer: {err.decode().rstrip()}"""

        self.assertEqual(log_msg, exp_msg)


    @patch('subprocess.run', side_effect=fake_ena_ftp_curl)
    def test_get_ena_ftp_url_curl_failed_3(self, mocked_run):
        # get_ena_ftp_url use subprocess to spawn a curl subprocess
        # we mock subprocess run to mimic the response of ENA
        # when fastq are available 2 calls to subprocess are performed
        # we cannot use a MagicMock

        # the second curl call return output with less than 2 lines
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        acc = 'ERR00003.BAD'
        to_download = ena.get_ena_ftp_url(acc)
        self.assertListEqual(to_download, [])


    @patch('subprocess.run', side_effect=fake_ena_ftp_curl)
    def test_get_ena_ftp_url_curl_failed_4(self, mocked_run):
        # get_ena_ftp_url use subprocess to spawn a curl subprocess
        # we mock subprocess run to mimic the response of ENA
        # when fastq are available 2 calls to subprocess are performed
        # we cannot use a MagicMock

        # the second curl call return output with no fastq_ftp nor fastq_md5 in header
        ena = ENA(self.seqdd_tmp_dir, self.bin_dir, self.logger)
        acc = 'ERR00004.BAD'
        with self.catch_log(log_name='seqdd') as log:
            to_download = ena.get_ena_ftp_url(acc)
            log_msg = log.get_value().rstrip()
        self.assertListEqual(to_download, [])
        self.assertEqual(log_msg,
                         f'No fastq files found for accession {acc}')
