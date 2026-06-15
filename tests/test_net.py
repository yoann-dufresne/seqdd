import gzip
import os
import tempfile
import shutil
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from seqdd.utils import net
from seqdd.errors import DownloadError
from tests import SeqddTest


class FakeResponse:
    """Minimal stand-in for a requests.Response usable as a context manager."""

    def __init__(self, status_code=200, content=b'', headers=None, chunks=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self._chunks = chunks if chunks is not None else [content]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def iter_content(self, chunk_size=1):
        yield from self._chunks

    def close(self):
        pass


class FakeSession:
    """Stand-in for requests.Session capturing the calls made by net.py."""

    def __init__(self, get_responses=None, head_responses=None):
        self._get = list(get_responses or [])
        self._head = list(head_responses or [])
        self.get_calls = []
        self.head_calls = []
        self.headers = {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        return self._get.pop(0)

    def head(self, url, **kwargs):
        self.head_calls.append((url, kwargs))
        return self._head.pop(0)

    def mount(self, *args):
        pass


class TestNetHelpers(SeqddTest):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='seqdd_net_')

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    @contextmanager
    def patched_session(self, session):
        with patch.object(net, '_build_session', return_value=session):
            yield

    # --- _normalize_url ---

    def test_normalize_url_adds_ftp_scheme(self):
        self.assertEqual(net._normalize_url('ftp.sra.ebi.ac.uk/vol1/x'), 'ftp://ftp.sra.ebi.ac.uk/vol1/x')

    def test_normalize_url_keeps_existing_scheme(self):
        self.assertEqual(net._normalize_url('https://a/b'), 'https://a/b')
        self.assertEqual(net._normalize_url('ftp://a/b'), 'ftp://a/b')

    # --- http_get / http_get_text ---

    def test_http_get_returns_body(self):
        session = FakeSession(get_responses=[FakeResponse(200, content=b'hello')])
        with self.patched_session(session):
            self.assertEqual(net.http_get('https://x/y'), b'hello')

    def test_http_get_text_decodes(self):
        session = FakeSession(get_responses=[FakeResponse(200, content='héllo'.encode())])
        with self.patched_session(session):
            self.assertEqual(net.http_get_text('https://x/y'), 'héllo')

    def test_http_get_raises_on_http_error(self):
        session = FakeSession(get_responses=[FakeResponse(404, content=b'')])
        with self.patched_session(session):
            with self.assertRaises(DownloadError):
                net.http_get('https://x/y')

    def test_http_get_raises_on_request_exception(self):
        session = FakeSession()
        session.get = MagicMock(side_effect=net.requests.RequestException('boom'))
        with self.patched_session(session):
            with self.assertRaises(DownloadError):
                net.http_get('https://x/y')

    # --- http_status ---

    def test_http_status_head_ok(self):
        session = FakeSession(head_responses=[FakeResponse(200)])
        with self.patched_session(session):
            self.assertEqual(net.http_status('https://x/y'), 200)

    def test_http_status_404_returned_not_raised(self):
        session = FakeSession(head_responses=[FakeResponse(404)])
        with self.patched_session(session):
            self.assertEqual(net.http_status('https://x/y'), 404)

    def test_http_status_falls_back_to_get_when_head_rejected(self):
        session = FakeSession(head_responses=[FakeResponse(405)], get_responses=[FakeResponse(200)])
        with self.patched_session(session):
            self.assertEqual(net.http_status('https://x/y'), 200)
        self.assertEqual(len(session.get_calls), 1)

    def test_http_status_unreachable_returns_zero(self):
        session = FakeSession()
        session.head = MagicMock(side_effect=net.requests.RequestException('no host'))
        with self.patched_session(session):
            self.assertEqual(net.http_status('https://x/y'), 0)

    # --- http_head_headers ---

    def test_http_head_headers_returns_headers(self):
        session = FakeSession(head_responses=[FakeResponse(200, headers={'Content-Disposition': 'attachment; filename=z.fa'})])
        with self.patched_session(session):
            status, headers = net.http_head_headers('https://x/y')
        self.assertEqual(status, 200)
        self.assertIn('Content-Disposition', headers)

    # --- download_file (http) ---

    def test_download_http_writes_content(self):
        dest = os.path.join(self.tmp, 'out.bin')
        session = FakeSession(get_responses=[FakeResponse(200, chunks=[b'abc', b'def'])])
        with self.patched_session(session):
            net.download_file('https://x/y', dest, resume=False)
        with open(dest, 'rb') as fh:
            self.assertEqual(fh.read(), b'abcdef')

    def test_download_http_resume_appends_on_206(self):
        dest = os.path.join(self.tmp, 'out.bin')
        with open(dest, 'wb') as fh:
            fh.write(b'abc')
        session = FakeSession(get_responses=[FakeResponse(206, chunks=[b'def'])])
        with self.patched_session(session):
            net.download_file('https://x/y', dest, resume=True)
        with open(dest, 'rb') as fh:
            self.assertEqual(fh.read(), b'abcdef')
        # The Range header must have been sent.
        self.assertEqual(session.get_calls[0][1]['headers'], {'Range': 'bytes=3-'})

    def test_download_http_restarts_when_server_ignores_range(self):
        dest = os.path.join(self.tmp, 'out.bin')
        with open(dest, 'wb') as fh:
            fh.write(b'stale')
        session = FakeSession(get_responses=[FakeResponse(200, chunks=[b'fresh'])])
        with self.patched_session(session):
            net.download_file('https://x/y', dest, resume=True)
        with open(dest, 'rb') as fh:
            self.assertEqual(fh.read(), b'fresh')

    def test_download_http_416_keeps_complete_file(self):
        dest = os.path.join(self.tmp, 'out.bin')
        with open(dest, 'wb') as fh:
            fh.write(b'complete')
        session = FakeSession(get_responses=[FakeResponse(416)])
        with self.patched_session(session):
            net.download_file('https://x/y', dest, resume=True)
        with open(dest, 'rb') as fh:
            self.assertEqual(fh.read(), b'complete')

    def test_download_http_raises_on_error_status(self):
        dest = os.path.join(self.tmp, 'out.bin')
        session = FakeSession(get_responses=[FakeResponse(500)])
        with self.patched_session(session):
            with self.assertRaises(DownloadError):
                net.download_file('https://x/y', dest, resume=False)

    # --- download_and_gzip ---

    def test_download_and_gzip_produces_gz_and_removes_plain(self):
        dest = os.path.join(self.tmp, 'rec.fa')
        session = FakeSession(get_responses=[FakeResponse(200, chunks=[b'>seq\nACGT\n'])])
        with self.patched_session(session):
            net.download_and_gzip('https://x/y', dest)
        self.assertFalse(os.path.exists(dest))
        self.assertTrue(os.path.exists(f'{dest}.gz'))
        with gzip.open(f'{dest}.gz', 'rb') as fh:
            self.assertEqual(fh.read(), b'>seq\nACGT\n')

    # --- FTP directory download ---

    def test_download_ftp_dir_recursive(self):
        # Remote tree: leaf/  ->  file a.txt + subdir sub/ -> b.txt
        tree = {
            '/base/GCF_1': [('a.txt', 'file'), ('sub', 'dir')],
            '/base/GCF_1/sub': [('b.txt', 'file')],
        }
        contents = {
            '/base/GCF_1/a.txt': b'AAA',
            '/base/GCF_1/sub/b.txt': b'BBB',
        }

        fake_ftp = MagicMock()

        def fake_list(ftp, remote_dir):
            return tree[remote_dir]

        def fake_retr(cmd, callback, blocksize=None):
            remote_path = cmd.split(' ', 1)[1]
            callback(contents[remote_path])

        fake_ftp.retrbinary.side_effect = fake_retr

        with patch.object(net, '_ftp_connect', return_value=fake_ftp), \
             patch.object(net, '_ftp_list', side_effect=fake_list):
            net.download_ftp_dir('ftp://ncbi/base/GCF_1/', self.tmp)

        with open(os.path.join(self.tmp, 'GCF_1', 'a.txt'), 'rb') as fh:
            self.assertEqual(fh.read(), b'AAA')
        with open(os.path.join(self.tmp, 'GCF_1', 'sub', 'b.txt'), 'rb') as fh:
            self.assertEqual(fh.read(), b'BBB')

    def test_download_ftp_file(self):
        dest = os.path.join(self.tmp, 'f.txt')
        fake_ftp = MagicMock()

        def fake_retr(cmd, callback, blocksize=None):
            callback(b'DATA')

        fake_ftp.retrbinary.side_effect = fake_retr
        with patch.object(net, '_ftp_connect', return_value=fake_ftp):
            net.download_file('ftp://host/path/f.txt', dest)
        with open(dest, 'rb') as fh:
            self.assertEqual(fh.read(), b'DATA')
