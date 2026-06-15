import logging
import os
import pickle
import sys
import tempfile
import time

from seqdd.utils.scheduler import JobManager, CmdLineJob, FunctionJob
from tests import SeqddTest


# Portable shell commands (work on POSIX shells and the Windows command prompt).
_OK_CMD = f'"{sys.executable}" -c "raise SystemExit(0)"'
_FAIL_CMD = f'"{sys.executable}" -c "raise SystemExit(1)"'


def _write_marker(path):
    """Module-level FunctionJob target (must be importable to be picklable under spawn)."""
    with open(path, 'w') as fh:
        fh.write('done')


class TestJobManagerOutcomes(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-sched-')

    def tearDown(self):
        self._tmp_dir.cleanup()

    def _run_jobs(self, jobs):
        manager = JobManager(logger=self.logger, max_process=4, log_folder=self._tmp_dir.name)
        with self.catch_log():
            manager.start()
            for job in jobs:
                manager.add_job(job)
            deadline = time.time() + 30
            while manager.remaining_jobs() > 0 and time.time() < deadline:
                time.sleep(0.05)
            manager.stop()
            manager.join()
        return manager

    def test_tracks_completed_failed_and_canceled(self):
        # The failing command cancels its child ; the succeeding one completes.
        ok = CmdLineJob(_OK_CMD, name='ok')
        bad = CmdLineJob(_FAIL_CMD, name='bad')
        child = CmdLineJob(_OK_CMD, parents=[bad], name='child')

        manager = self._run_jobs([ok, bad, child])

        completed = {j.name for j in manager.completed_jobs}
        failed = {j.name for j in manager.failed_jobs}
        canceled = {j.name for j in manager.canceled_jobs}

        self.assertIn('ok', completed)
        self.assertIn('bad', failed)
        self.assertIn('child', canceled)
        # A canceled child must never be counted as completed
        self.assertNotIn('child', completed)
        # Everything is accounted for and the queue drained
        self.assertEqual(manager.remaining_jobs(), 0)

    def test_all_success(self):
        jobs = [CmdLineJob(_OK_CMD, name=f'ok{i}') for i in range(3)]
        manager = self._run_jobs(jobs)
        self.assertEqual(len(manager.completed_jobs), 3)
        self.assertEqual(len(manager.failed_jobs), 0)
        self.assertEqual(len(manager.canceled_jobs), 0)

    def test_function_job_payload_is_picklable(self):
        # Under the spawn start method (Windows/macOS), only (target, args, log_file) is sent to
        # the child. It must be picklable; nothing referencing the job (lambdas, locks) is sent.
        job = FunctionJob(func_to_run=_write_marker, func_args=('/tmp/marker',), name='pickle')
        pickle.dumps((job.to_run, job.args, job.log_file))

    def test_function_job_runs_and_reports_success(self):
        marker = os.path.join(self._tmp_dir.name, 'marker.txt')
        job = FunctionJob(func_to_run=_write_marker, func_args=(marker,), name='marker')
        manager = self._run_jobs([job])
        self.assertIn('marker', {j.name for j in manager.completed_jobs})
        self.assertTrue(os.path.isfile(marker))
