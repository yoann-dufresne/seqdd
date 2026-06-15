import logging
import os
import tempfile
from unittest import mock

from seqdd.errors import DownloadError
from seqdd.register.sources.refseq import RefSeq, move_and_clean
from seqdd.register.data_type.refseq import Refseq
from seqdd.register.datatype_manager import DataTypeManager
from tests import SeqddTest


def _index_row(acc, ftp):
    """Build a tab-separated assembly_summary row with `acc` in col 1 and `ftp` in col 20."""
    cols = [f'c{i}' for i in range(22)]
    cols[0] = acc
    cols[19] = ftp
    return '\t'.join(cols)


INDEX_LINES = [
    '#   See ftp://ftp.ncbi.nlm.nih.gov/... assembly summary',
    '#assembly_accession\tbioproject\ttaxid',
    _index_row('GCF_000001215.4', 'ftp://server/path/GCF_000001215.4_dm6'),
]


class TestRefSeqSource(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix='seqdd-refseq-')
        self._data = tempfile.TemporaryDirectory(prefix='seqdd-refdata-')

    def tearDown(self):
        self._tmp.cleanup()
        self._data.cleanup()

    def _write_index(self):
        index_path = os.path.join(self._tmp.name, 'assembly_summary_refseq.txt')
        with open(index_path, 'w') as f:
            f.write('\n'.join(INDEX_LINES) + '\n')
        return index_path

    def test_get_index_parses_and_skips_comments(self):
        # The index starts with '#' comment lines that must be skipped, and the parsed
        # index must be cached (index_ready) to avoid re-downloading.
        src = RefSeq(self._tmp.name, self.logger)
        self._write_index()
        # download_file is mocked out: the index file is already on disk, only parsing is tested.
        with mock.patch('seqdd.utils.net.download_file'):
            ok = src.get_index()
        self.assertTrue(ok)
        self.assertTrue(src.index_ready)
        self.assertEqual(src.index, {'GCF_000001215.4': 'ftp://server/path/GCF_000001215.4_dm6'})

    def test_get_index_download_failure(self):
        src = RefSeq(self._tmp.name, self.logger)
        with mock.patch('seqdd.utils.net.download_file', side_effect=DownloadError('boom')):
            ok = src.get_index()
        self.assertFalse(ok)
        self.assertFalse(src.index_ready)

    def test_move_and_clean_moves_tree(self):
        # move_and_clean must relocate the whole tree without raising (the source dir is
        # consumed by the move, so no extra cleanup must be attempted).
        acc_dir = os.path.join(self._tmp.name, 'acc_src')
        os.makedirs(acc_dir)
        with open(os.path.join(acc_dir, 'genome.fna.gz'), 'w') as f:
            f.write('data')
        outdir = os.path.join(self._data.name, 'GCF_000001215.4')

        move_and_clean(acc_dir, outdir)

        self.assertTrue(os.path.isfile(os.path.join(outdir, 'genome.fna.gz')))
        self.assertFalse(os.path.exists(acc_dir))

    def test_jobs_from_accessions_builds_jobs(self):
        src = RefSeq(self._tmp.name, self.logger)
        src.index = {'GCF_000001215.4': 'ftp://server/path/GCF_000001215.4_dm6'}
        src.index_ready = True

        jobs = src.jobs_from_accessions(['GCF_000001215.4', 'GCF_999999999.9'], self._data.name)
        names = [j.name for j in jobs]

        self.assertIn('refseq_GCF_000001215.4_download', names)
        self.assertIn('refseq_GCF_000001215.4_move', names)
        self.assertFalse(any('999999999' in n for n in names))
        download = next(j for j in jobs if j.name.endswith('_download'))
        self.assertIn('ftp://server/path/GCF_000001215.4_dm6', download.args[0])

    def test_jobs_skips_already_downloaded(self):
        src = RefSeq(self._tmp.name, self.logger)
        src.index = {'GCF_000001215.4': 'ftp://server/path/GCF_000001215.4_dm6'}
        src.index_ready = True
        os.makedirs(os.path.join(self._data.name, 'GCF_000001215.4'))

        jobs = src.jobs_from_accessions(['GCF_000001215.4'], self._data.name)

        self.assertEqual(jobs, [])

    def test_validate_accession(self):
        src = RefSeq(self._tmp.name, self.logger)
        self.assertEqual(src.validate_accession('GCF_000001215.4'), 'Reference')
        self.assertEqual(src.validate_accession('GCA_000001215.4'), 'Invalid')
        self.assertEqual(src.validate_accession('nonsense'), 'Invalid')

    def test_filter_valid_keeps_known_gcf(self):
        src = RefSeq(self._tmp.name, self.logger)
        src.index = {'GCF_000001215.4': 'ftp://server/path/GCF_000001215.4_dm6'}
        src.index_ready = True

        valid = src.filter_valid(['GCF_000001215.4', 'GCF_999999999.9', 'GCA_000001215.4'])

        self.assertEqual(valid, ['GCF_000001215.4'])

    def test_latest_genbank_equivalent_from_ena(self):
        # The most recent GCA version is resolved from ENA (version-less query -> latest).
        src = RefSeq(self._tmp.name, self.logger)
        ena_xml = '<ASSEMBLY_SET><ASSEMBLY accession="GCA_000001405.29" alias="GRCh38.p14"></ASSEMBLY></ASSEMBLY_SET>'
        with mock.patch('seqdd.utils.net.http_get_text', return_value=ena_xml):
            gca = src.latest_genbank_equivalent('GCF_000001405.40')
        self.assertEqual(gca, 'GCA_000001405.29')

    def test_latest_genbank_equivalent_falls_back_to_index(self):
        # If ENA cannot be queried, fall back to the GenBank assembly paired in the RefSeq index.
        src = RefSeq(self._tmp.name, self.logger)
        src.gca_index = {'GCF_000001215.4': 'GCA_000001215.4'}
        with mock.patch('seqdd.utils.net.http_get_text', side_effect=DownloadError('err')):
            gca = src.latest_genbank_equivalent('GCF_000001215.4')
        self.assertEqual(gca, 'GCA_000001215.4')


class TestRefSeqContainer(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix='seqdd-refseq-')
        self._data = tempfile.TemporaryDirectory(prefix='seqdd-refdata-')

    def tearDown(self):
        self._tmp.cleanup()
        self._data.cleanup()

    def _source(self):
        src = RefSeq(self._tmp.name, self.logger)
        src.index = {'GCF_000001215.4': 'ftp://server/path/GCF_000001215.4_dm6'}
        src.index_ready = True
        return src

    def test_refseq_registered_as_datatype(self):
        # The container must be auto-discovered as the `refseq` type, wired to a RefSeq source.
        mng = DataTypeManager(self.logger)
        types = mng.get_data_types()
        self.assertIn('refseq', types)
        self.assertIsInstance(types['refseq'].source, RefSeq)

    def test_get_download_jobs_delegates_to_source(self):
        container = Refseq(self._source(), self.logger)
        container.add_data(['GCF_000001215.4'])

        jobs = container.get_download_jobs(self._data.name)

        self.assertTrue(any(j.name == 'refseq_GCF_000001215.4_download' for j in jobs))

    def test_filter_valid_keeps_well_formed_and_present(self):
        container = Refseq(self._source(), self.logger)

        # The GenBank announcement queries ENA; mock it so the test stays offline.
        with mock.patch('seqdd.utils.net.http_get_text', side_effect=DownloadError('offline')):
            valid = container.filter_valid(['GCF_000001215.4', 'GCF_999999999.9', 'not-a-gcf'])

        # well-formed + present kept; well-formed but absent dropped by the source;
        # malformed dropped by the container's pattern check
        self.assertEqual(valid, ['GCF_000001215.4'])

    def test_filter_valid_announces_gca_on_stdout(self):
        src = self._source()
        src.gca_index = {'GCF_000001215.4': 'GCA_000001215.4'}
        container = Refseq(src, self.logger)
        ena_xml = '<ASSEMBLY accession="GCA_000001215.4" alias="Release 6 plus ISO1 MT"></ASSEMBLY>'

        with mock.patch('seqdd.utils.net.http_get_text', return_value=ena_xml):
            with self.catch_io(out=True) as (out, _err):
                valid = container.filter_valid(['GCF_000001215.4'])
        output = out.getvalue()

        self.assertEqual(valid, ['GCF_000001215.4'])
        self.assertIn('GCA_000001215.4', output)
        self.assertIn('ENA', output)
        self.assertIn('-t assemblies', output)
