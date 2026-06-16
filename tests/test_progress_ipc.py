"""
Feasibility test for step 2 of the download progress bar (issue #3): per-download, byte-level
progress reporting.

Step 1 (merged separately) added a job-count bar. Step 2 needs each download to report the bytes
it pulls, so a per-file, multi-line progress display can be drawn. The hard part is that downloads
run in **separate processes** started with a non-fork multiprocessing context (``forkserver`` /
``spawn`` — the very context the real :class:`~seqdd.utils.scheduler.JobManager` uses), so the byte
counts must travel back to the parent over an inter-process channel.

This test wires the new ``net.download_file(..., progress=...)`` hook to a
:class:`multiprocessing.Queue` and checks, against the local controllable HTTP server, that the
parent process receives an **accurate per-job byte stream** from real download subprocesses — i.e.
that the step-2 mechanism is sound. The actual multi-line rendering (consuming these counts) is the
remaining, low-risk UI work.
"""

import logging
import os
import queue as queue_mod
import shutil
import tempfile
import time
from collections import defaultdict

from tests import SeqddTest
from tests.support.controllable_http_server import ControllableHTTPServer

from seqdd.utils import net
from seqdd.utils.checksum import sha256sum
from seqdd.utils.scheduler import _MP_CONTEXT, FunctionJob, JobManager


def _download_reporting(url: str, dest: str, report_queue, job_id: int) -> None:
    """
    Module-level (picklable) download worker, run in a subprocess under the non-fork context.

    Downloads ``url`` to ``dest`` and pushes ``(job_id, n_bytes)`` on ``report_queue`` for each
    written chunk, then a final ``(job_id, None)`` sentinel so the parent knows the job is done.

    The reporting callback is built **inside** the child, so nothing unpicklable crosses the
    process boundary: only this top-level function and its (picklable) arguments are sent.
    """
    net.download_file(url, dest, resume=False, progress=lambda n: report_queue.put((job_id, n)))
    report_queue.put((job_id, None))


class TestByteProgressIPC(SeqddTest):

    @classmethod
    def setUpClass(cls):
        # A few MiB so the body is streamed over several 1 MiB chunks (several progress events);
        # a non-multiple size catches any off-by-one in byte accounting.
        cls._dir = tempfile.mkdtemp(prefix='seqdd-progress-ipc-')
        cls.source = os.path.join(cls._dir, 'payload.bin')
        cls.size = 5 * (1 << 20) + 1234
        with open(cls.source, 'wb') as fh:
            fh.write(os.urandom(cls.size))
        cls.source_sha = sha256sum(cls.source)
        cls.server = ControllableHTTPServer(cls.source).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
        shutil.rmtree(cls._dir, ignore_errors=True)

    def test_parallel_downloads_report_bytes_to_parent(self):
        n_jobs = 3
        out_dir = tempfile.mkdtemp(prefix='seqdd-progress-out-', dir=self._dir)
        report_queue = _MP_CONTEXT.Queue()

        procs = []
        dests = {}
        for job_id in range(n_jobs):
            dest = os.path.join(out_dir, f'job{job_id}.bin')
            dests[job_id] = dest
            proc = _MP_CONTEXT.Process(
                target=_download_reporting,
                args=(self.server.url(), dest, report_queue, job_id),
            )
            proc.start()
            procs.append(proc)

        # Parent side: aggregate per-job byte counts until every job sends its sentinel.
        # FIFO-per-producer guarantees a job's chunk events all arrive before its sentinel.
        received = defaultdict(int)
        finished = 0
        while finished < n_jobs:
            try:
                job_id, nbytes = report_queue.get(timeout=60)
            except queue_mod.Empty:
                self.fail('Timed out waiting for byte-progress events from the download subprocesses')
            if nbytes is None:
                finished += 1
            else:
                self.assertGreater(nbytes, 0)
                received[job_id] += nbytes

        for proc in procs:
            proc.join(timeout=60)
            self.assertEqual(proc.exitcode, 0, 'a download subprocess exited with an error')

        # Each job must have reported exactly the file size, and downloaded the file intact.
        for job_id in range(n_jobs):
            self.assertEqual(
                received[job_id], self.size,
                f'job {job_id} reported {received[job_id]} bytes, expected {self.size}',
            )
            self.assertEqual(sha256sum(dests[job_id]), self.source_sha)

        # The counts collected above are exactly what a per-file progress bar would consume:
        # bytes-received / total -> percentage, per job. Here every job is complete (100%).
        for job_id in range(n_jobs):
            percent = int(100 * received[job_id] / self.size)
            self.assertEqual(percent, 100)

    def test_jobmanager_wires_progress_end_to_end(self):
        # The integrated path: the JobManager detects that net.download_file accepts a `progress`
        # argument, creates the queue, tags the jobs, and exposes the events via poll_progress().
        out_dir = tempfile.mkdtemp(prefix='seqdd-jm-', dir=self._dir)
        logger = logging.getLogger('seqdd')
        manager = JobManager(logger=logger, max_process=2, log_folder=out_dir)
        jobs = [
            FunctionJob(func_to_run=net.download_file,
                        func_args=(self.server.url(), os.path.join(out_dir, f'jm{i}.bin')),
                        name=f'dl{i}')
            for i in range(2)
        ]

        received = defaultdict(int)
        with self.catch_log():
            manager.start()
            for job in jobs:
                manager.add_job(job)
            self.assertIsNotNone(manager.progress_queue, 'the JobManager should have created a queue')
            deadline = time.time() + 60
            while manager.remaining_jobs() > 0 and time.time() < deadline:
                for job_id, n_bytes in manager.poll_progress():
                    if n_bytes is not None:
                        received[job_id] += n_bytes
                time.sleep(0.02)
            for job_id, n_bytes in manager.poll_progress():
                if n_bytes is not None:
                    received[job_id] += n_bytes
            manager.stop()
            manager.join()

        self.assertEqual(set(received), {'dl0', 'dl1'})
        for job_id in ('dl0', 'dl1'):
            self.assertEqual(received[job_id], self.size)
