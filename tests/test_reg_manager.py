import logging
import tempfile
import os

from seqdd.register.data_type.logan import Logan
from seqdd.register.data_type.read_archives import ReadArchives
from seqdd.register.sources.ena import ENA
from seqdd.register.sources.url_server import UrlServer
from tests import SeqddTest
from seqdd.register.reg_manager import Register, get_accessions_from_source, save_accesions_to_file, create_register
from seqdd.register.datatype_manager import DataTypeManager
from seqdd.register.data_type import DataSource, DataContainer


class MockSource(DataSource):
    def __init__(self):
        self.tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-')
        super().__init__(self.tmp_dir.name, self.tmp_dir.name, logging.getLogger('seqdd'))

    def jobs_from_accessions(self, accessions: list[str], datadir: str) -> list:
        return []


class TestRegister(SeqddTest):

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')
        cls.cwd = os.getcwd()
        cls.data_type_mng = DataTypeManager(cls.logger)
        cls.data_sources = cls.data_type_mng.get_data_types().keys()

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-')
        os.chdir(self._tmp_dir.name)


    def tearDown(self):
        self._tmp_dir.cleanup()
        os.chdir(self.cwd)


    def test_init(self):
        reg = Register(self.logger)
        self.assertEqual(reg.data_containers.keys(),
                             self.data_sources)

        reg = Register(self.logger, dirpath=self._tmp_dir.name)
        # The directory is empty

        reg = Register(self.logger, regfile=self.find_data('register.reg'))
        reg_file = {'readarchives': ReadArchives(ENA(self._tmp_dir, self._tmp_dir, self.logger), self.logger),
                    'logan': Logan(UrlServer(self._tmp_dir, self._tmp_dir, self.logger), self.logger)}
        reg_file['readarchives'].add_data(['ENA_000001', 'SRA000001', 'GCA_000002'])
        reg_file['logan'].add_data(['SRR6246166_contigs'])
        for type in self.data_sources:
            with self.subTest(data_source=type):
                container = reg.data_containers[type]
                if type in reg_file:
                    self.assertSetEqual(container.data, reg_file[type].data)
                else:
                    self.assertSetEqual(container.data, set())


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
        self.assertEqual(log_msg,
                         f'Register {register_name} does not exist.')

        os.mkdir(register_name)
        acc_by_src = {ds: f"ACC000{i}" for i, ds in enumerate(self.data_sources)}
        for ds, acc in acc_by_src.items():
            with open(os.path.join(register_name, f"{ds}.txt"), 'w') as f:
                f.write(acc + '\n')

        resp = reg.load_from_dir(register_name)
        self.assertTrue(resp)
        for ds, container in reg.data_containers.items():
            with self.subTest(data_source=ds):
                excp_value = set()
                excp_value.add(acc_by_src[ds])
                self.assertSetEqual(container.data, excp_value)
                

    def test_load_from_file(self):
        register_name = 'register.reg'
        reg = Register(self.logger)
        reg.load_from_file(self.find_data(register_name))
        reg_file = {'readarchives': {'ENA_000001', 'SRA000001', 'GCA_000002'},
                    'logan': {'SRR6246166_contigs'}}
        for ds in self.data_sources:
            with self.subTest(data_source=ds):
                accs = reg.data_containers[ds]
                if ds in reg_file:
                    self.assertSetEqual(accs.data, reg_file[ds])
                else:
                    self.assertSetEqual(accs.data, set())


    def test_load_from_file_bad_header(self):
        register_name = 'register.reg'
        no_header_reg_file = """readarchives\t3
            SRA000001
            GCA_000002
            ENA_000001
            logan\t1
            SRR6246166_contigs"""
        register_path = os.path.join(self._tmp_dir.name, register_name)
        with open(register_path, 'w') as f:
            f.write(no_header_reg_file)

        with self.catch_log(log_name='seqdd') as log:
            reg = Register(self.logger, regfile=register_path)
            log_msg = log.get_value().rstrip()
        self.assertEqual(log_msg,
                         'Missing version number at the beginning of the reg file. Skipping the loading')
        self.assertEqual(reg.data_containers.keys(),
                             self.data_sources)


    def test_load_from_file_bad_major(self):
        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        bad_major_reg_file = """version 1.0
            readarchives\t3
            SRA000001
            GCA_000002
            ENA_000001
            logan\t1
            SRR6246166_contigs"""
        with open(register_path, 'w') as f:
            f.write(bad_major_reg_file)

        with self.catch_log(log_name='seqdd') as log:
            reg = Register(self.logger, regfile=register_path)
            log_msg = log.get_value().rstrip()
        self.assertEqual(log_msg, 'Incompatible versions. '
                                  'Your register is major version 1 while the tool awaits version 0. '
                                  'Skipping the loading')
        self.assertEqual(reg.data_containers.keys(),
                             self.data_sources)


    def test_load_from_file_higher_minor(self):
        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        bad_major_reg_file = """version 0.5
            readarchives\t3
            SRA000001
            GCA_000002
            ENA_000001
            logan\t1
            SRR6246166_contigs"""
        with open(register_path, 'w') as f:
            f.write(bad_major_reg_file)

        with self.catch_log(log_name='seqdd') as log:
            reg = Register(self.logger, regfile=register_path)
            log_msg = log.get_value().rstrip()
        self.assertEqual(log_msg, 'Incompatible versions. '
                                  'Your register is major version 0.5 while the tool awaits maximum version 0.1 . '
                                  'Skipping the loading')
        self.assertEqual(reg.data_containers.keys(),
                             self.data_sources)


    def test_load_from_file_lower_minor(self):
        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        bad_major_reg_file = """version 0.5
            readarchives\t3
            SRA000001
            GCA_000002
            ENA_000001
            logan\t1
            SRR6246166_contigs"""
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
        
        dict_expected = {'readarchives': {'SRA000001', 'GCA_000002', 'ENA_000001'},
                            'logan': {'SRR6246166_contigs'},
                            'url': set(),
                            'assemblies': set()
                        }
        for k in reg.data_containers.keys():
            if k in dict_expected:
                self.assertSetEqual(reg.data_containers[k].data, dict_expected[k])
        
        self.assertEqual(log_msg, f'Data from {register_path} successfully loaded')


    def test_remove_accession(self):
        reg = Register(self.logger)
        ds = 'nimportnoik'
        with self.catch_log() as log:
            reg.remove_accession(ds, 'SRA000001')
            log_msg = log.get_value().rstrip()
        self.assertEqual(log_msg, f'Data type {ds} not found in the register.')

        acc = 'nimportnaoik'
        with self.catch_log() as log:
            reg.remove_accession('readarchives', acc)
            log_msg = log.get_value().rstrip()
        self.assertEqual(log_msg, f'Accession {acc} not found in readarchives')

        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        reg_file = """version 0.0
            readarchives\t4
            SRA000001
            GCA_000001
            GCA_000002
            ENA_000001
            logan\t1
            SRR6246166_contigs"""
        with open(register_path, 'w') as f:
            f.write(reg_file)
        reg = Register(self.logger, regfile=register_path)
        ds = 'readarchives'
        acc = 'GCA_000001'
        self.assertSetEqual(reg.data_containers[ds].data, {'SRA000001', 'GCA_000001', 'GCA_000002', 'ENA_000001'})
        reg.remove_accession(ds, acc)
        self.assertSetEqual(reg.data_containers[ds].data, {'SRA000001', 'GCA_000002', 'ENA_000001'})


    def test_filter_accessions(self):
        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        reg_file = """version 0.0
            readarchives\t4
            SRA000001
            GCA_000001
            GCA_000002
            ENA_000001
            logan\t1
            SRR6246166_contigs"""
        with open(register_path, 'w') as f:
            f.write(reg_file)
        reg = Register(self.logger, regfile=register_path)
        accs = reg.filter_accessions('readarchives', ['^GCA'])
        self.assertListEqual(['GCA_000001', 'GCA_000002'], sorted(accs))
        accs = reg.filter_accessions('readarchives', ['.*01$'])
        self.assertListEqual(['ENA_000001', 'GCA_000001', 'SRA000001'], sorted(accs))
        accs = reg.filter_accessions('readarchives', ['02$', '^GCA'])
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
            readarchives\t3
            SRA000001
            GCA_000002
            ENA_000001
            logan\t1
            SRR6246166_contigs"""
        with open(register_path, 'w') as f:
            f.write(reg_file)
        reg = Register(self.logger, regfile=register_path)

        register_saved = 'saved_register.reg'
        new_register_path = os.path.join(self._tmp_dir.name, register_saved)
        reg.save_to_file(new_register_path)
        reg_saved = Register(self.logger, regfile=new_register_path)
        self.assertDictEqual(reg.data_containers, reg_saved.data_containers)


    def test_save_to_dir(self):
        register_name = 'register.reg'
        register_path = os.path.join(self._tmp_dir.name, register_name)
        reg_file = """version 0.0
            readarchives\t3
            SRA000001
            GCA_000002
            ENA_000001
            logan\t1
            SRR6246166_contigs"""
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
        self.assertDictEqual(reg.data_containers, reg_saved.data_containers)


class TestSrcRegister(SeqddTest):


    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('seqdd')
        cls.cwd = os.getcwd()
        cls.data_type_mng = DataTypeManager(cls.logger)
        cls.data_sources = cls.data_type_mng.get_data_types().keys()

    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(prefix='seqdd-')
        os.chdir(self._tmp_dir.name)


    def tearDown(self):
        self._tmp_dir.cleanup()
        os.chdir(self.cwd)


    def test_get_accessions_from_source(self):
        src_path = 'readarchives.txt'
        src = get_accessions_from_source(src_path)
        self.assertSetEqual(src, set())

        accs = {'ACC00001', 'ACC00002'}
        with open(src_path, 'w') as f:
            f.write('\n'.join(accs))
            f.write('\n')

        src = get_accessions_from_source(src_path)
        self.assertSetEqual(src, accs)


    def test_save_accessions_to_source(self):
        src_path = 'readarchives.txt'
        accs = {'ACC00001', 'ACC00002'}
        save_accesions_to_file(src_path, accs)
        src = get_accessions_from_source(src_path)
        self.assertSetEqual(src, accs)


    def test_create_register(self):
        dir_path = os.path.join('level1', 'register')
        create_register(dir_path, logger=self.logger)
        self.assertTrue(os.path.isdir(dir_path))

        with self.catch_log() as log:
            with self.assertRaises(FileExistsError) as err:
                create_register(dir_path, logger=self.logger)
            log_msg = log.get_value().rstrip()
        exp_msg = f"A register is already present at location {dir_path}"
        self.assertEqual(str(err.exception),
                         exp_msg)
        self.assertEqual(log_msg,
                         exp_msg)

        create_register(dir_path, logger=self.logger, force=True)
        self.assertTrue(os.path.isdir(dir_path))
