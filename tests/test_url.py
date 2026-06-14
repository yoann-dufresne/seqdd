import logging
import tempfile
from unittest import mock

from seqdd.register.data_type.url import URL
from seqdd.register.sources.url_server import UrlServer
from tests import SeqddTest


class TestURL(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-')
        self._data_dir = tempfile.TemporaryDirectory(prefix='seqdd-data-')

    def tearDown(self):
        self._tmp_dir.cleanup()
        self._data_dir.cleanup()

    def test_get_download_jobs_forwards_datadir(self):
        # The URL container must forward the download directory to its source,
        # otherwise UrlServer.jobs_from_accessions raises a TypeError (missing datadir).
        source = UrlServer(self._tmp_dir.name, self.logger)
        container = URL(source, self.logger)
        container.add_data(['http://example.com/file.fa'])

        with mock.patch.object(UrlServer, 'get_filename', return_value='file.fa'):
            jobs = container.get_download_jobs(self._data_dir.name)

        self.assertEqual(len(jobs), 1)
        # The download command must write into the requested data directory.
        self.assertIn(self._data_dir.name, jobs[0].cmd)
