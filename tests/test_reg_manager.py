import logging
import tempfile
import os

from tests import SeqddTest
from seqdd.register.reg_manager import Register
from seqdd.register.src_manager import DataSourceLoader

class TestRegister(SeqddTest):


    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')
        cls.cwd = os.getcwd()
        cls.data_sources = DataSourceLoader().keys()

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-')
        os.chdir(self._tmp_dir.name)


    def tearDown(self):
        self._tmp_dir.cleanup()
        os.chdir(self.cwd)


    def test_init(self):
        reg = Register(self.logger)
        self.assertDictEqual(reg.acc_by_src,
                             {ds: set() for ds in self.data_sources})

        reg = Register(self.logger, dirpath=self._tmp_dir.name)
        # The directory is empty
        # so acc_by_src also we test with data in test_load_from_dir
        self.assertDictEqual(reg.acc_by_src,
                             {ds: set() for ds in self.data_sources})

        reg = Register(self.logger, regfile=self.find_data('register.reg'))
        reg_file = {'ena': {'ENA_000001'},
                    'sra': {'SRA000001'},
                    'ncbi': {'GCA_000001', 'GCA_000002'}}
        for ds in self.data_sources:
            with self.subTest(data_source=ds):
                accs = reg.acc_by_src[ds]
                if ds in reg_file:
                    self.assertSetEqual(accs, reg_file[ds])
                else:
                    self.assertSetEqual(accs, set())


    def test_load_from_dir(self):
        register_name = 'register'
        open(register_name, 'w').close()
        reg = Register(self.logger)
        with self.catch_log(log_name='seqdd') as log:
            try:
                resp = reg.load_from_dir(register_name)
            finally:
                os.unlink(register_name)
            log_msg = log.get_value().rstrip()
        self.assertFalse(resp)
        self.assertDictEqual(reg.acc_by_src,
                             {ds: set() for ds in self.data_sources})
        self.assertEqual(log_msg,
                         f'Register {register_name} does not exist.')

        os.mkdir(register_name)
        acc_by_src = {ds: f"ACC000{i}" for i, ds in enumerate(self.data_sources)}
        for ds, acc in acc_by_src.items():
            with open(os.path.join(register_name, f"{ds}.txt"), 'w') as f:
                f.write(acc + '\n')

        resp = reg.load_from_dir(register_name)
        self.assertTrue(resp)
        for ds, accs in reg.acc_by_src.items():
            with self.subTest(data_source=ds):
                excp_value = set()
                excp_value.add(acc_by_src[ds])
                self.assertSetEqual(accs, excp_value)


    def test_load_from_file(self):
        register_name = 'register.reg'
        reg = Register(self.logger)
        reg.load_from_file(self.find_data(register_name))
        reg_file = {'ena': {'ENA_000001'},
                    'sra': {'SRA000001'},
                    'ncbi': {'GCA_000001', 'GCA_000002'}}
        for ds in self.data_sources:
            with self.subTest(data_source=ds):
                accs = reg.acc_by_src[ds]
                if ds in reg_file:
                    self.assertSetEqual(accs, reg_file[ds])
                else:
                    self.assertSetEqual(accs, set())


    def test_load_from_file_bad_header(self):
        register_name = 'register.reg'
        no_header_reg_file = """sra\t1
SRA000001
ncbi\t2
GCA_000001
GCA_000002
ena\t1
ENA_000001"""
        register_path = os.path.join(self._tmp_dir.name, register_name)
        with open(register_path, 'w') as f:
            f.write(no_header_reg_file)

        with self.catch_log(log_name='seqdd') as log:
            reg = Register(self.logger, regfile=register_path)
            log_msg = log.get_value().rstrip()
        self.assertEqual(log_msg,
                         'Missing version number at the beginning of the reg file. Skipping the loading')
        self.assertDictEqual(reg.acc_by_src,
                             {ds: set() for ds in self.data_sources})


    def test_load_from_file_bad_major(self):
        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        bad_major_reg_file = """version 1.0
sra\t1
SRA000001
ncbi\t2
GCA_000001
GCA_000002
ena\t1
ENA_000001"""
        with open(register_path, 'w') as f:
            f.write(bad_major_reg_file)

        with self.catch_log(log_name='seqdd') as log:
            reg = Register(self.logger, regfile=register_path)
            log_msg = log.get_value().rstrip()
        self.assertEqual(log_msg, 'Incompatible versions. '
                                  'Your register is major version 1 while the tool awaits version 0. '
                                  'Skipping the loading')
        self.assertDictEqual(reg.acc_by_src,
                             {ds: set() for ds in self.data_sources})


    def test_load_from_file_higher_minor(self):
        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        bad_major_reg_file = """version 0.5
    sra\t1
    SRA000001
    ncbi\t2
    GCA_000001
    GCA_000002
    ena\t1
    ENA_000001"""
        with open(register_path, 'w') as f:
            f.write(bad_major_reg_file)

        with self.catch_log(log_name='seqdd') as log:
            reg = Register(self.logger, regfile=register_path)
            log_msg = log.get_value().rstrip()
        self.assertEqual(log_msg, 'Incompatible versions. '
                                  'Your register is major version 0.5 while the tool awaits maximum version 0.0 . '
                                  'Skipping the loading')
        self.assertDictEqual(reg.acc_by_src,
                             {ds: set() for ds in self.data_sources})


    def test_load_from_file_lower_minor(self):
        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        bad_major_reg_file = """version 0.5
    sra\t1
    SRA000001
    ncbi\t2
    GCA_000001
    GCA_000002
    ena\t1
    ENA_000001"""
        with open(register_path, 'w') as f:
            f.write(bad_major_reg_file)

        with self.catch_log(log_name='seqdd') as log:
            try:
                level = self.logger.getEffectiveLevel()
                self.logger.setLevel(logging.DEBUG)
                Register.minor_version = 7
                reg = Register(self.logger, regfile=register_path)
            finally:
                Register.minor_version = 0
                self.logger.setLevel(level)
            log_msg = log.get_value().rstrip()
        self.assertDictEqual(reg.acc_by_src,
                             {'sra': {'SRA000001'},
                                 'ncbi': {'GCA_000001','GCA_000002'},
                                 'ena' : {'ENA_000001'},
                                 'logan': set(),
                                 'url': set()
                              })
        self.assertEqual(log_msg, f'Data from {register_path} successfully loaded')


    def test_remove_accession(self):
        reg = Register(self.logger)
        ds = 'nimportnoik'
        with self.catch_log() as log:
            reg.remove_accession(ds, 'SRA000001')
            log_msg = log.get_value().rstrip()
        self.assertEqual(log_msg, f'Source {ds} not found in the register.')

        acc = 'nimportnaoik'
        with self.catch_log() as log:
            reg.remove_accession('sra', acc)
            log_msg = log.get_value().rstrip()
        self.assertEqual(log_msg, f'Accession {acc} not found in sra')

        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        reg_file = """version 0.0
    sra\t1
    SRA000001
    ncbi\t2
    GCA_000001
    GCA_000002
    ena\t1
    ENA_000001"""
        with open(register_path, 'w') as f:
            f.write(reg_file)
        reg = Register(self.logger, regfile=register_path)
        ds = 'ncbi'
        acc = 'GCA_000001'
        self.assertSetEqual(reg.acc_by_src[ds], {'GCA_000001', 'GCA_000002'})
        reg.remove_accession(ds, acc)
        self.assertSetEqual(reg.acc_by_src[ds], {'GCA_000002'})


    def test_filter_accessions(self):
        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        reg_file = """version 0.0
    sra\t1
    SRA000001
    ncbi\t2
    GCA_000001
    GCA_000002
    ena\t1
    ENA_000001"""
        with open(register_path, 'w') as f:
            f.write(reg_file)
        reg = Register(self.logger, regfile=register_path)
        accs = reg.filter_accessions('ncbi', ['^GCA'])
        self.assertListEqual(['GCA_000001', 'GCA_000002'], sorted(accs))
        accs = reg.filter_accessions('ncbi', ['.*01$'])
        self.assertListEqual(['GCA_000001'], accs)
        accs = reg.filter_accessions('ncbi', ['02$', '^GCA'])
        # filter_accessions use match so 02$ does not match any acc
        # the 2 acc match the '^GCA' patten
        self.assertListEqual(['GCA_000001', 'GCA_000002'], sorted(accs))

        with self.catch_log() as log:
            accs = reg.filter_accessions('nimportnaoik', ['^GCA'])
            log_msg = log.get_value().rstrip()
        self.assertListEqual(accs, [])
        self.assertEqual(log_msg, 'Source nimportnaoik not found in the register.')


    def test_save_to_file(self):
        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        reg_file = """version 0.0
    sra\t1
    SRA000001
    ncbi\t2
    GCA_000001
    GCA_000002
    ena\t1
    ENA_000001"""
        with open(register_path, 'w') as f:
            f.write(reg_file)
        reg = Register(self.logger, regfile=register_path)

        register_saved = 'saved_register.reg'
        new_register_path = os.path.join(self._tmp_dir.name, register_saved)
        reg.save_to_file(new_register_path)
        reg_saved = Register(self.logger, regfile=new_register_path)
        self.assertDictEqual(reg.acc_by_src, reg_saved.acc_by_src)


    def test_save_to_dir(self):
        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        reg_file = """version 0.0
            sra\t1
            SRA000001
            ncbi\t2
            GCA_000001
            GCA_000002
            ena\t1
            ENA_000001"""
        with open(register_path, 'w') as f:
            f.write(reg_file)
        reg = Register(self.logger, regfile=register_path)

        reg_dir = 'register'
        new_register_path = os.path.join(self._tmp_dir.name, reg_dir)
        # The dir does not exists
        with self.catch_log() as log:
            resp = reg.save_to_dir(new_register_path)
            log_msg = log.get_value().rstrip()
        self.assertFalse(resp)
        self.assertEqual(log_msg,
                         f'Register {new_register_path} does not exist. Save aborted...')

        # the dir exists
        os.mkdir(new_register_path)
        try:
            level = self.logger.getEffectiveLevel()
            self.logger.setLevel(logging.DEBUG)
            with self.catch_log() as log:
                resp = reg.save_to_dir(new_register_path)
                log_msg = log.get_value().rstrip()
        finally:
            self.logger.setLevel(level)
        self.assertTrue(resp)
        self.assertEqual(log_msg,
                         f'Register saved to {new_register_path}')

        reg_saved = Register(self.logger, dirpath=new_register_path)
        self.assertDictEqual(reg.acc_by_src, reg_saved.acc_by_src)
