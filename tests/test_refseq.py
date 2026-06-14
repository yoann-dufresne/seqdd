import logging
import os
import tempfile
from types import SimpleNamespace
from unittest import mock

from seqdd.register.sources.refseq import RefSeq
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
        with mock.patch('subprocess.run', return_value=SimpleNamespace(returncode=0, stdout=b'', stderr=b'')):
            ok = src.get_index()
        self.assertTrue(ok)
        self.assertTrue(src.index_ready)
        self.assertEqual(src.index, {'GCF_000001215.4': 'ftp://server/path/GCF_000001215.4_dm6'})

    def test_get_index_download_failure(self):
        src = RefSeq(self._tmp.name, self.logger)
        with mock.patch('subprocess.run', return_value=SimpleNamespace(returncode=1, stdout=b'', stderr=b'boom')):
            ok = src.get_index()
        self.assertFalse(ok)
        self.assertFalse(src.index_ready)

    def test_move_and_clean_moves_tree(self):
        # move_and_clean must relocate the whole tree without raising (the source dir is
        # consumed by the move, so no extra cleanup must be attempted).
        src = RefSeq(self._tmp.name, self.logger)
        acc_dir = os.path.join(self._tmp.name, 'acc_src')
        os.makedirs(acc_dir)
        with open(os.path.join(acc_dir, 'genome.fna.gz'), 'w') as f:
            f.write('data')
        outdir = os.path.join(self._data.name, 'GCF_000001215.4')

        src.move_and_clean(acc_dir, outdir)

        self.assertTrue(os.path.isfile(os.path.join(outdir, 'genome.fna.gz')))
        self.assertFalse(os.path.exists(acc_dir))

    def test_jobs_from_accessions_builds_jobs(self):
        src = RefSeq(self._tmp.name, self.logger)
        src.index = {'GCF_000001215.4': 'ftp://server/path/GCF_000001215.4_dm6'}
        src.index_ready = True

        jobs = src.jobs_from_accessions(['GCF_000001215.4', 'GCF_999999999.9'], self._data.name)
        names = [j.name for j in jobs]

        self.assertIn('refseq_GCF_000001215.4_wget', names)
        self.assertIn('refseq_GCF_000001215.4_move', names)
        self.assertFalse(any('999999999' in n for n in names))
        wget = next(j for j in jobs if j.name.endswith('_wget'))
        self.assertIn('ftp://server/path/GCF_000001215.4_dm6', wget.cmd)

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
