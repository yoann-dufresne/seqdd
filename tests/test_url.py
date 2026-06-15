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
        # The download job must write into the requested data directory.
        url, filepath = jobs[0].args
        self.assertEqual(url, 'http://example.com/file.fa')
        self.assertIn(self._data_dir.name, filepath)

    def test_filter_valid_delegates_to_source(self):
        # The URL container must delegate validation to its source; without this,
        # `seqdd add -t url` crashes (the base DataContainer has no filter_valid).
        source = UrlServer(self._tmp_dir.name, self.logger)
        container = URL(source, self.logger)
        with mock.patch.object(UrlServer, 'filter_valid', return_value=['http://example.com/f']) as m:
            valid = container.filter_valid(['http://example.com/f'])
        m.assert_called_once_with(['http://example.com/f'])
        self.assertEqual(valid, ['http://example.com/f'])

    def test_source_filter_valid_accepts_200_any_http_version(self):
        source = UrlServer(self._tmp_dir.name, self.logger)
        # A 200 status (works for HTTP/1.1 and HTTP/2) keeps the URL.
        with mock.patch('seqdd.utils.net.http_status', return_value=200):
            self.assertEqual(source.filter_valid(['https://x/y']), ['https://x/y'])
        # A non-200 status drops it.
        with mock.patch('seqdd.utils.net.http_status', return_value=404):
            self.assertEqual(source.filter_valid(['https://x/y']), [])
        # An unreachable host (status 0) drops it.
        with mock.patch('seqdd.utils.net.http_status', return_value=0):
            self.assertEqual(source.filter_valid(['https://x/y']), [])
