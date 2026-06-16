import logging
import os
import sys
import tempfile
from argparse import Namespace
from unittest import mock

import json

from seqdd.__main__ import on_export, on_list, on_remove, on_status, on_verify, parse_cmd
from seqdd.register.reg_manager import Register, create_register
from seqdd.utils.manifest import build_manifest, write_manifest
from tests import SeqddTest


class TestOnList(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')
        cls.cwd = os.getcwd()

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-')
        os.chdir(self._tmp_dir.name)

    def tearDown(self):
        os.chdir(self.cwd)
        self._tmp_dir.cleanup()

    def _make_register(self):
        loc = os.path.join(self._tmp_dir.name, '.register')
        reg = create_register(loc, self.logger)
        reg.data_containers['readarchives'].add_data(['SRR000001', 'SRR000002', 'ERR000003'])
        reg.save_to_dir(loc)
        return loc

    def test_on_list_filters_by_regex(self):
        # `list -r` must only display accessions matching the regular expression(s).
        loc = self._make_register()
        args = Namespace(register_location=loc, regular_expressions=['^SRR'], type=None)

        with self.catch_io(out=True) as (out, _err):
            on_list(args, self.logger)
        output = out.getvalue()

        self.assertIn('SRR000001', output)
        self.assertIn('SRR000002', output)
        self.assertNotIn('ERR000003', output)


class TestAccessionArgParsing(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')

    def _parse(self, argv):
        with mock.patch.object(sys, 'argv', ['seqdd', *argv]):
            return parse_cmd(self.logger)

    def test_add_repeated_accession_flags_accumulate(self):
        # Regression: `add -a X -a Y` must register BOTH accessions, not just the last one.
        args = self._parse(['add', '-t', 'sequences', '-a', 'U00096.3', '-a', 'MN908947'])
        self.assertCountEqual(args.accessions, ['U00096.3', 'MN908947'])

    def test_add_single_flag_with_several_values(self):
        # `add -a X Y` (one flag, several values) must keep working too.
        args = self._parse(['add', '-t', 'sequences', '-a', 'U00096.3', 'MN908947'])
        self.assertCountEqual(args.accessions, ['U00096.3', 'MN908947'])

    def test_remove_repeated_accession_flags_accumulate(self):
        # Same fix on `remove`, which shares the multi-accession option.
        args = self._parse(['remove', '-a', 'SRR000001', '-a', 'SRR000002'])
        self.assertCountEqual(args.accessions, ['SRR000001', 'SRR000002'])


class TestOnRemove(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')
        cls.cwd = os.getcwd()

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-')
        os.chdir(self._tmp_dir.name)

    def tearDown(self):
        os.chdir(self.cwd)
        self._tmp_dir.cleanup()

    def _make_register(self):
        loc = os.path.join(self._tmp_dir.name, '.register')
        reg = create_register(loc, self.logger)
        reg.data_containers['readarchives'].add_data(['SRR000001', 'SRR000002', 'ERR000003'])
        reg.save_to_dir(loc)
        return loc

    def test_on_remove_with_regex(self):
        # `remove -a` accessions may be regular expressions (as advertised in the help).
        loc = self._make_register()
        args = Namespace(register_location=loc, accessions=['^SRR'], type=None)

        on_remove(args, self.logger)

        reloaded = Register(self.logger, dirpath=loc)
        self.assertSetEqual(reloaded.data_containers['readarchives'].data, {'ERR000003'})


class TestOnStatus(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')
        cls.cwd = os.getcwd()

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-')
        os.chdir(self._tmp_dir.name)

    def tearDown(self):
        os.chdir(self.cwd)
        self._tmp_dir.cleanup()

    def _make(self):
        loc = os.path.join(self._tmp_dir.name, '.register')
        reg = create_register(loc, self.logger)
        reg.data_containers['assemblies'].add_data(['GCA_000000001.1', 'GCA_000000002.1'])
        reg.save_to_dir(loc)
        datadir = os.path.join(self._tmp_dir.name, 'data')
        os.makedirs(os.path.join(datadir, 'GCA_000000001.1'))
        with open(os.path.join(datadir, 'GCA_000000001.1', 'genome.fa'), 'w') as fw:
            fw.write('>s\nACGT\n')
        return loc, datadir

    def test_reports_downloaded_and_missing(self):
        loc, datadir = self._make()
        args = Namespace(register_location=loc, download_directory=datadir, type=None)
        with self.catch_io(out=True) as (out, _err):
            on_status(args, self.logger)
        output = out.getvalue()
        self.assertIn('1/2 downloaded', output)
        self.assertIn('GCA_000000002.1', output)  # missing one is listed

    def test_flat_layout_not_tracked(self):
        loc = os.path.join(self._tmp_dir.name, '.register')
        reg = create_register(loc, self.logger)
        reg.data_containers['url'].add_data(['https://example.org/a.fa'])
        reg.save_to_dir(loc)
        args = Namespace(register_location=loc, download_directory='data', type='url')
        with self.catch_io(out=True) as (out, _err):
            on_status(args, self.logger)
        self.assertIn('not tracked', out.getvalue())


class TestOnVerify(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-')
        self.datadir = os.path.join(self._tmp_dir.name, 'data')
        os.makedirs(os.path.join(self.datadir, 'acc'))
        with open(os.path.join(self.datadir, 'acc', 'genome.fa'), 'w') as fw:
            fw.write('>s\nACGT\n')

    def tearDown(self):
        self._tmp_dir.cleanup()

    def test_verify_ok(self):
        write_manifest(self.datadir)
        args = Namespace(download_directory=self.datadir, manifest=None)
        with self.catch_io(out=True) as (out, _err):
            on_verify(args, self.logger)  # must not raise
        self.assertIn('1 ok', out.getvalue())

    def test_verify_corrupt_exits_nonzero(self):
        write_manifest(self.datadir)
        with open(os.path.join(self.datadir, 'acc', 'genome.fa'), 'w') as fw:
            fw.write('tampered')
        args = Namespace(download_directory=self.datadir, manifest=None)
        with self.assertRaises(SystemExit) as cm:
            with self.catch_io(out=True):
                on_verify(args, self.logger)
        self.assertNotEqual(cm.exception.code, 0)

    def test_verify_without_manifest_exits(self):
        args = Namespace(download_directory=os.path.join(self._tmp_dir.name, 'nope'), manifest=None)
        with self.assertRaises(SystemExit):
            on_verify(args, self.logger)

    def test_verify_with_external_manifest(self):
        # Capture a manifest, then verify the data directory against that explicit lock file.
        manifest = build_manifest(self.datadir)
        manifest_path = os.path.join(self._tmp_dir.name, 'shared.lock.json')
        with open(manifest_path, 'w') as fw:
            json.dump(manifest, fw)
        args = Namespace(download_directory=self.datadir, manifest=manifest_path)
        with self.catch_io(out=True) as (out, _err):
            on_verify(args, self.logger)
        self.assertIn('1 ok', out.getvalue())


class TestOnExport(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')
        cls.cwd = os.getcwd()

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-')
        os.chdir(self._tmp_dir.name)

    def tearDown(self):
        os.chdir(self.cwd)
        self._tmp_dir.cleanup()

    def test_export_with_lock_copies_manifest(self):
        loc = os.path.join(self._tmp_dir.name, '.register')
        reg = create_register(loc, self.logger)
        reg.data_containers['assemblies'].add_data(['GCA_000000001.1'])
        reg.save_to_dir(loc)
        datadir = os.path.join(self._tmp_dir.name, 'data')
        os.makedirs(os.path.join(datadir, 'GCA_000000001.1'))
        with open(os.path.join(datadir, 'GCA_000000001.1', 'genome.fa'), 'w') as fw:
            fw.write('>s\nACGT\n')
        write_manifest(datadir)

        out_reg = os.path.join(self._tmp_dir.name, 'shared.reg')
        args = Namespace(register_location=loc, output_register=out_reg,
                         download_directory=datadir, with_lock=True)
        on_export(args, self.logger)

        self.assertTrue(os.path.isfile(out_reg))
        self.assertTrue(os.path.isfile(os.path.join(self._tmp_dir.name, 'shared.lock.json')))

    def test_export_without_lock_does_not_create_lock(self):
        loc = os.path.join(self._tmp_dir.name, '.register')
        reg = create_register(loc, self.logger)
        reg.data_containers['assemblies'].add_data(['GCA_000000001.1'])
        reg.save_to_dir(loc)
        out_reg = os.path.join(self._tmp_dir.name, 'plain.reg')
        args = Namespace(register_location=loc, output_register=out_reg,
                         download_directory='data', with_lock=False)
        on_export(args, self.logger)
        self.assertTrue(os.path.isfile(out_reg))
        self.assertFalse(os.path.isfile(os.path.join(self._tmp_dir.name, 'plain.lock.json')))
