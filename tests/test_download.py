import logging
import os
import tempfile

from seqdd.register.datatype_manager import DataTypeManager
from seqdd.register.reg_manager import Register
from seqdd.utils.download import DownloadManager
from tests import SeqddTest


class TestDryRun(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix='seqdd-dl-')

    def tearDown(self):
        self._tmp.cleanup()

    def test_dry_run_creates_nothing_and_returns_zeros(self):
        reg = Register(self.logger)
        reg.data_containers['assemblies'].add_data(['GCA_000000001.1'])
        dtm = DataTypeManager(self.logger, tmpdir=self._tmp.name)
        dm = DownloadManager(reg, dtm, self.logger)

        datadir = os.path.join(self._tmp.name, 'data')
        logdir = os.path.join(self._tmp.name, 'logs')
        with self.catch_log():
            result = dm.download_to(datadir, logdir, max_process=2, dry_run=True)

        self.assertEqual(result, {'completed': 0, 'failed': 0, 'canceled': 0})
        # No side effects: neither the data nor the log directory is created
        self.assertFalse(os.path.exists(datadir))
        self.assertFalse(os.path.exists(logdir))
