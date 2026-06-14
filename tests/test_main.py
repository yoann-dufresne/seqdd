import logging
import os
import tempfile
from argparse import Namespace

from seqdd.__main__ import on_list
from seqdd.register.reg_manager import Register, create_register
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
