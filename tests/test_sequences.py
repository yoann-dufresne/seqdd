import logging
import os
import tempfile
from unittest import mock

from seqdd.register.sources.ena import ENA
from seqdd.register.data_type.sequences import Sequences
from seqdd.register.datatype_manager import DataTypeManager
from tests import SeqddTest


class TestSequences(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix='seqdd-seq-')
        self._data = tempfile.TemporaryDirectory(prefix='seqdd-seqdata-')

    def tearDown(self):
        self._tmp.cleanup()
        self._data.cleanup()

    def _container(self):
        return Sequences(ENA(self._tmp.name, self.logger), self.logger)

    def test_read_source_accepts_insdc_rejects_others(self):
        for acc in ['U00096.3', 'MN908947', 'AY123456', 'CABVMW010000001']:
            self.assertIsNotNone(Sequences.read_source(acc), acc)
        for acc in ['GCA_003543015.1', 'GCF_000001215.4', 'SRR000001', 'nonsense', '']:
            self.assertIsNone(Sequences.read_source(acc), acc)

    def test_registered_as_datatype(self):
        # Auto-discovered as the `sequences` type, wired to an ENA source.
        types = DataTypeManager(self.logger).get_data_types()
        self.assertIn('sequences', types)
        self.assertIsInstance(types['sequences'].source, ENA)

    def test_filter_valid_prefilters_then_checks_ena_fasta(self):
        container = self._container()
        # Malformed/assembly accessions are dropped by the pattern; ENA (FASTA HEAD) confirms the rest.
        with mock.patch.object(ENA, 'valid_sequence_accessions', return_value=['U00096.3']) as m:
            valid = container.filter_valid(['U00096.3', 'GCA_003543015.1', 'nonsense'])
        m.assert_called_once_with(['U00096.3'])
        self.assertEqual(valid, ['U00096.3'])

    def test_get_download_jobs_builds_fasta_jobs(self):
        container = self._container()
        container.add_data(['U00096.3'])

        jobs = container.get_download_jobs(self._data.name)
        names = [j.name for j in jobs]

        self.assertIn('ena_U00096.3_download', names)
        self.assertIn('ena_U00096.3_gzip', names)
        self.assertIn('ena_U00096.3_move', names)
        curl = next(j for j in jobs if j.name.endswith('_download'))
        self.assertIn('ena/browser/api/fasta/U00096.3', curl.cmd)

    def test_get_download_jobs_skips_already_downloaded(self):
        container = self._container()
        container.add_data(['U00096.3'])
        os.makedirs(os.path.join(self._data.name, 'U00096.3'))

        jobs = container.get_download_jobs(self._data.name)

        self.assertEqual(jobs, [])
