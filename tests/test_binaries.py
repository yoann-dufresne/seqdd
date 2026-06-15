import logging

from seqdd.register.reg_manager import Register
from seqdd.utils.binaries import missing_binaries, required_binaries_for
from tests import SeqddTest


class TestMissingBinaries(SeqddTest):

    def test_detects_absent_binary(self):
        missing = missing_binaries(['sh', 'definitely-not-a-real-binary-xyz'])
        self.assertIn('definitely-not-a-real-binary-xyz', missing)
        self.assertNotIn('sh', missing)

    def test_all_present_returns_empty(self):
        self.assertEqual(missing_binaries(['sh']), [])

    def test_deduplicates(self):
        # A repeated missing name must appear only once
        self.assertEqual(
            missing_binaries(['definitely-not-a-real-binary-xyz', 'definitely-not-a-real-binary-xyz']),
            ['definitely-not-a-real-binary-xyz'],
        )


class TestRequiredBinariesFor(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')

    def test_empty_register_needs_nothing(self):
        reg = Register(self.logger)
        self.assertEqual(required_binaries_for(reg), set())

    def test_union_over_non_empty_types(self):
        reg = Register(self.logger)
        reg.data_containers['assemblies'].add_data(['GCA_000001635.9'])  # ENA -> curl/gzip/md5sum
        reg.data_containers['refseq'].add_data(['GCF_000001635.9'])      # RefSeq -> curl/wget
        self.assertEqual(required_binaries_for(reg), {'curl', 'gzip', 'md5sum', 'wget'})

    def test_ignores_empty_types(self):
        reg = Register(self.logger)
        reg.data_containers['url'].add_data(['https://example.org/file.fa'])  # UrlServer -> curl only
        self.assertEqual(required_binaries_for(reg), {'curl'})
