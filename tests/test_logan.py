import logging
import os
import tempfile
from unittest import mock

from seqdd.register.data_type.logan import Logan
from seqdd.register.sources.url_server import UrlServer
from tests import SeqddTest


class TestLogan(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-')
        self._data_dir = tempfile.TemporaryDirectory(prefix='seqdd-data-')

    def tearDown(self):
        self._tmp_dir.cleanup()
        self._data_dir.cleanup()

    def _container(self):
        source = UrlServer(self._tmp_dir.name, self.logger)
        container = Logan(source, self.logger)
        container.add_data(['SRR000001_contigs'])
        return container

    def test_get_download_jobs_skips_already_downloaded(self):
        # UrlServer writes downloaded files as 'url<idx>_<filename>'. The presence of such
        # a file must make Logan skip the accession instead of downloading it again.
        container = self._container()
        downloaded = os.path.join(self._data_dir.name, 'url0_SRR000001.contigs.fa.zst')
        open(downloaded, 'w').close()

        with mock.patch.object(UrlServer, 'get_filename', return_value='SRR000001.contigs.fa.zst'):
            jobs = container.get_download_jobs(self._data_dir.name)

        self.assertEqual(jobs, [])

    def test_get_download_jobs_creates_job_when_absent(self):
        # Guard against over-skipping: a job must be created when nothing is downloaded yet.
        container = self._container()

        with mock.patch.object(UrlServer, 'get_filename', return_value='SRR000001.contigs.fa.zst'):
            jobs = container.get_download_jobs(self._data_dir.name)

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].name, 'logan_SRR000001_contigs_download')
