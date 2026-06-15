import os
import tempfile

from seqdd.utils.checksum import sha256sum
from seqdd.utils.manifest import (
    MANIFEST_NAME,
    build_manifest,
    load_manifest,
    load_manifest_file,
    verify_against,
    verify_manifest,
    write_manifest,
)
from tests import SeqddTest


class TestChecksum(SeqddTest):

    def test_sha256sum_known_value(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, 'f.txt')
            with open(p, 'wb') as fw:
                fw.write(b'hello')
            self.assertEqual(
                sha256sum(p),
                '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824',
            )


class TestManifest(SeqddTest):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix='seqdd-manifest-')
        self.datadir = self._tmp.name
        # Accession-subdir layout (ENA / RefSeq / sequences / assemblies)
        os.makedirs(os.path.join(self.datadir, 'GCA_000146045.2'))
        with open(os.path.join(self.datadir, 'GCA_000146045.2', 'genome.fa.gz'), 'wb') as fw:
            fw.write(b'ACGT' * 10)
        # Flat layout (url / logan)
        with open(os.path.join(self.datadir, 'url0_contigs.fa.zst'), 'wb') as fw:
            fw.write(b'unitigs')

    def tearDown(self):
        self._tmp.cleanup()

    def test_build_records_both_layouts(self):
        manifest = build_manifest(self.datadir)
        paths = {e['path'] for e in manifest['files']}
        self.assertIn(os.path.join('GCA_000146045.2', 'genome.fa.gz'), paths)
        self.assertIn('url0_contigs.fa.zst', paths)
        self.assertIn('seqdd_version', manifest)
        for entry in manifest['files']:
            self.assertIn('sha256', entry)
            self.assertIn('size', entry)

    def test_write_and_load_roundtrip(self):
        written = write_manifest(self.datadir)
        self.assertTrue(os.path.isfile(os.path.join(self.datadir, MANIFEST_NAME)))
        loaded = load_manifest(self.datadir)
        self.assertEqual(written, loaded)
        # The manifest never records itself
        paths = {e['path'] for e in loaded['files']}
        self.assertNotIn(MANIFEST_NAME, paths)

    def test_verify_all_ok(self):
        write_manifest(self.datadir)
        result = verify_manifest(self.datadir)
        self.assertEqual(result['corrupt'], [])
        self.assertEqual(result['missing'], [])
        self.assertEqual(result['extra'], [])
        self.assertEqual(len(result['ok']), 2)

    def test_verify_detects_corrupt(self):
        write_manifest(self.datadir)
        with open(os.path.join(self.datadir, 'url0_contigs.fa.zst'), 'wb') as fw:
            fw.write(b'tampered')
        result = verify_manifest(self.datadir)
        self.assertIn('url0_contigs.fa.zst', result['corrupt'])

    def test_verify_detects_missing(self):
        write_manifest(self.datadir)
        os.remove(os.path.join(self.datadir, 'url0_contigs.fa.zst'))
        result = verify_manifest(self.datadir)
        self.assertIn('url0_contigs.fa.zst', result['missing'])

    def test_verify_detects_extra(self):
        write_manifest(self.datadir)
        with open(os.path.join(self.datadir, 'unexpected.txt'), 'wb') as fw:
            fw.write(b'x')
        result = verify_manifest(self.datadir)
        self.assertIn('unexpected.txt', result['extra'])

    def test_verify_without_manifest_raises(self):
        with self.assertRaises(FileNotFoundError):
            verify_manifest(self.datadir)

    def test_verify_against_external_manifest(self):
        manifest = build_manifest(self.datadir)
        result = verify_against(manifest, self.datadir)
        self.assertEqual(result['corrupt'], [])
        self.assertEqual(result['missing'], [])
        self.assertEqual(len(result['ok']), 2)

    def test_load_manifest_file_missing_raises(self):
        with self.assertRaises(FileNotFoundError):
            load_manifest_file(os.path.join(self.datadir, 'nope.json'))
