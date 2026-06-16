"""
Large-scale download tests: integrity of relatively big files, resume after interruption, servers
honoring/ignoring Range, transient 5xx, parallel downloads through the scheduler, and the full
`seqdd add -t url` + `download` CLI pipeline with an inter-run resume.

Everything is driven by a local, controllable HTTP server (tests/support) so the scenarios are
deterministic and run on every OS. The default file size is moderate to keep CI fast; scale it up
with the environment variables below for a heavier run:

    SEQDD_TEST_DOWNLOAD_MB     size of the served file in MiB (default 16)
    SEQDD_TEST_DOWNLOAD_FILES  number of files for the parallel scenario (default 3)

All artifacts live under temporary directories that are removed on teardown.
"""

import gzip
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time

from seqdd.errors import DownloadError
from seqdd.utils import net
from seqdd.utils.checksum import sha256sum
from seqdd.utils.scheduler import JobManager, FunctionJob
from seqdd.utils.manifest import MANIFEST_NAME
from tests import SeqddTest
from tests.support.controllable_http_server import ControllableHTTPServer

_MiB = 1 << 20
_SIZE = int(os.environ.get('SEQDD_TEST_DOWNLOAD_MB', '16')) * _MiB
_FILES = int(os.environ.get('SEQDD_TEST_DOWNLOAD_FILES', '3'))
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestLargeDownload(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')
        # One on-disk random source file, generated once and served (streamed) to every test.
        cls._class_dir = tempfile.mkdtemp(prefix='seqdd-large-src-')
        cls.source = os.path.join(cls._class_dir, 'payload.bin')
        with open(cls.source, 'wb') as fh:
            remaining = _SIZE
            while remaining > 0:
                block = min(_MiB, remaining)
                fh.write(os.urandom(block))
                remaining -= block
        cls.total = os.path.getsize(cls.source)
        cls.source_sha = sha256sum(cls.source)
        cls.server = ControllableHTTPServer(cls.source).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
        shutil.rmtree(cls._class_dir, ignore_errors=True)

    def setUp(self):
        # Reset the server behavior and counters before each scenario.
        srv = self.server
        srv.ignore_range = False
        srv.drops_remaining = 0
        srv.drop_after = 0
        srv.fail_times = 0
        srv.reset_counters()
        self.tmp = tempfile.mkdtemp(prefix='seqdd-large-')

    def tearDown(self):
        # Remove every downloaded artifact (the point of the test: download big, then delete).
        shutil.rmtree(self.tmp, ignore_errors=True)

    # --- helpers ---

    def _assert_complete(self, dest):
        self.assertTrue(os.path.isfile(dest))
        self.assertEqual(os.path.getsize(dest), self.total)
        self.assertEqual(sha256sum(dest), self.source_sha)

    def _run_jobs(self, jobs, timeout=180):
        manager = JobManager(logger=self.logger, max_process=4, log_folder=self.tmp)
        with self.catch_log():
            manager.start()
            for job in jobs:
                manager.add_job(job)
            deadline = time.time() + timeout
            while manager.remaining_jobs() > 0 and time.time() < deadline:
                time.sleep(0.05)
            manager.stop()
            manager.join()
        return manager

    def _seqdd(self, *args):
        env = dict(os.environ)
        env['PYTHONPATH'] = _REPO_ROOT + os.pathsep + env.get('PYTHONPATH', '')
        return subprocess.run([sys.executable, '-m', 'seqdd', *args],
                              cwd=_REPO_ROOT, env=env, capture_output=True, text=True)

    # --- scenarios ---

    def test_01_full_download_integrity(self):
        dest = os.path.join(self.tmp, 'plain.bin')
        net.download_file(self.server.url(), dest, resume=False)
        self._assert_complete(dest)

    def test_02_auto_resume_within_single_call(self):
        # A single download_file call must survive a mid-stream cut and resume by itself.
        self.server.drops_remaining = 1
        self.server.drop_after = self.total // 3
        dest = os.path.join(self.tmp, 'resumed.bin')
        net.download_file(self.server.url(), dest, resume=True)
        self._assert_complete(dest)
        self.assertGreaterEqual(self.server.get_count, 2)
        # The retry carried a Range header resuming from the bytes already written.
        self.assertTrue(any(r and r.startswith('bytes=') for r in self.server.ranges))

    def test_03_resume_on_next_call_after_failure(self):
        # With retries=0 a single call does not self-resume: the first call fails leaving a partial
        # file, the next call resumes it (mirrors re-running `seqdd download`).
        self.server.drops_remaining = 1
        self.server.drop_after = max(net._CHUNK_SIZE, self.total // 4)
        dest = os.path.join(self.tmp, 'twocall.bin')

        with self.assertRaises(DownloadError):
            net.download_file(self.server.url(), dest, resume=True, retries=0)
        partial = os.path.getsize(dest)
        self.assertGreater(partial, 0)
        self.assertLess(partial, self.total)

        net.download_file(self.server.url(), dest, resume=True, retries=0)
        self._assert_complete(dest)
        self.assertEqual(self.server.ranges[-1], f'bytes={partial}-')

    def test_04_server_honors_range(self):
        # Pre-seed a valid prefix; the server answers 206 and only the missing bytes are appended.
        dest = os.path.join(self.tmp, 'honor.bin')
        cut = self.total // 2
        with open(self.source, 'rb') as src, open(dest, 'wb') as out:
            out.write(src.read(cut))

        net.download_file(self.server.url(), dest, resume=True)
        self._assert_complete(dest)
        self.assertEqual(self.server.get_count, 1)
        self.assertEqual(self.server.ranges[-1], f'bytes={cut}-')

    def test_05_server_ignores_range(self):
        # Pre-seed a stale prefix; the server ignores Range and returns 200, so the file is rewritten.
        dest = os.path.join(self.tmp, 'ignore.bin')
        cut = self.total // 2
        with open(dest, 'wb') as out:
            out.write(b'\x00' * cut)
        self.server.ignore_range = True

        net.download_file(self.server.url(), dest, resume=True)
        self._assert_complete(dest)

    def test_06_already_complete_is_noop(self):
        dest = os.path.join(self.tmp, 'complete.bin')
        shutil.copyfile(self.source, dest)

        net.download_file(self.server.url(), dest, resume=True)
        self._assert_complete(dest)
        self.assertEqual(self.server.ranges[-1], f'bytes={self.total}-')

    def test_07_recovers_from_transient_5xx(self):
        # A transient 503 is absorbed by the urllib3 retry policy of the session.
        self.server.fail_times = 1
        dest = os.path.join(self.tmp, 'retry5xx.bin')
        net.download_file(self.server.url(), dest, resume=False)
        self._assert_complete(dest)
        self.assertGreaterEqual(self.server.get_count, 2)

    def test_08_download_and_gzip_large(self):
        dest = os.path.join(self.tmp, 'rec.fa')
        net.download_and_gzip(self.server.url(), dest)
        self.assertFalse(os.path.exists(dest))
        gz = f'{dest}.gz'
        self.assertTrue(os.path.isfile(gz))
        # Decompress and compare to the source.
        plain = os.path.join(self.tmp, 'rec.decompressed')
        with gzip.open(gz, 'rb') as src, open(plain, 'wb') as out:
            shutil.copyfileobj(src, out, length=_MiB)
        self.assertEqual(sha256sum(plain), self.source_sha)

    def test_09_parallel_downloads_via_scheduler(self):
        # Several big downloads run in parallel through the real JobManager (spawn-safe FunctionJob).
        dests = [os.path.join(self.tmp, f'parallel_{i}.bin') for i in range(_FILES)]
        jobs = [FunctionJob(func_to_run=net.download_file, func_args=(self.server.url(), d),
                            name=f'dl_{i}') for i, d in enumerate(dests)]
        manager = self._run_jobs(jobs)
        self.assertEqual(len(manager.completed_jobs), _FILES)
        self.assertEqual(len(manager.failed_jobs), 0)
        for dest in dests:
            self._assert_complete(dest)

    def test_10_cli_pipeline_with_resume_across_runs(self):
        # End-to-end: init + add (HEAD validation) + download. The first download is cut enough
        # times to fail; re-running download resumes the partial url file and completes. Finally
        # the provenance manifest must match the source.
        reg = os.path.join(self.tmp, '.register')
        data = os.path.join(self.tmp, 'data')
        logs = os.path.join(self.tmp, 'logs')
        url = self.server.url('genome.bin')

        self.assertEqual(self._seqdd('init', '--register-location', reg).returncode, 0)
        added = self._seqdd('add', '-t', 'url', '-a', url, '--register-location', reg)
        self.assertEqual(added.returncode, 0, added.stderr)

        # Make the first `download` fail: drop on every attempt of the single download_file call.
        self.server.drops_remaining = net.DEFAULT_RETRIES + 1
        self.server.drop_after = max(net._CHUNK_SIZE, self.total // 8)

        first = self._seqdd('download', '--register-location', reg, '-d', data, '--log-directory', logs)
        self.assertNotEqual(first.returncode, 0)

        # A partial file is left behind and resumed on the second run (drops are now exhausted).
        partials = [f for f in os.listdir(data) if f.startswith('url0_')]
        self.assertEqual(len(partials), 1)
        self.assertGreater(os.path.getsize(os.path.join(data, partials[0])), 0)

        second = self._seqdd('download', '--register-location', reg, '-d', data, '--log-directory', logs)
        self.assertEqual(second.returncode, 0, second.stderr)

        downloaded = os.path.join(data, partials[0])
        self.assertEqual(sha256sum(downloaded), self.source_sha)

        # The manifest records the downloaded file with the right sha256.
        with open(os.path.join(data, MANIFEST_NAME)) as fh:
            manifest = json.load(fh)
        entry = next(e for e in manifest['files'] if e['path'] == partials[0])
        self.assertEqual(entry['sha256'], self.source_sha)
        self.assertEqual(entry['size'], self.total)
