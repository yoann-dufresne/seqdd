import logging
import tempfile
import time

from seqdd.utils.scheduler import JobManager, CmdLineJob
from tests import SeqddTest


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
        # `false` fails -> its child is canceled ; `true` succeeds.
        ok = CmdLineJob('true', name='ok')
        bad = CmdLineJob('false', name='bad')
        child = CmdLineJob('true', parents=[bad], name='child')

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
        jobs = [CmdLineJob('true', name=f'ok{i}') for i in range(3)]
        manager = self._run_jobs(jobs)
        self.assertEqual(len(manager.completed_jobs), 3)
        self.assertEqual(len(manager.failed_jobs), 0)
        self.assertEqual(len(manager.canceled_jobs), 0)
