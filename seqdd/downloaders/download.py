import seqdd.downloaders.ncbi as ncbi
import seqdd.downloaders.sra as sra
from shutil import rmtree
from os import path, listdir, rename, remove
from os import makedirs
from sys import stderr
import platform
import subprocess
from threading import Lock
import time

from seqdd.utils.scheduler import JobManager, CmdLineJob, FunctionJob


# -------------------- Global download manager --------------------

class DownloadManager:

    def __init__(self, register, bindir='bin'):
        self.register = register
        ncbi, sra, wget = self.get_downloaders(bindir)

    def get_downloaders(self, bindir):
        self.ncbi = self.sra = wget = None

        if len(self.register.subregisters['ncbi']) > 0:
            self.ncbi = check_binaries(['datasets'], bindir, ncbi.download_datasets_software)
        if len(self.register.subregisters['sra']) > 0:
            self.sra = check_binaries(['prefetch', 'fasterq-dump'], bindir, sra.download_sra_toolkit)

        return self.ncbi, self.sra, wget

    def download_to(self, datadir, max_process=8):
        '''
        Download data from the register to datadir. Downloading can be parallelised

            Parameters:
                datadir (pathlike object): Path to the directory where the data should be downloaded.
                max_process (int): Maximum number of processus to run in parallel.
        '''

        # Creates the data directory if not existing
        makedirs(datadir, exist_ok=True)

        # --- ncbi genomes ---
        ncbi_jobs = []
        ncbi_reg = self.register.subregisters['ncbi']
        if len(ncbi_reg) > 0:
            downloadable = self.ncbi is not None
            if downloadable:
                ncbi_jobs.extend(ncbi.jobs_from_accessions(ncbi_reg, datadir, self.ncbi['datasets']))

            else:
                print(f'ncbi genomes cannot be downloaded because the datasets tool is absent from the system. Skipping {len(ncbi_reg)} datasets.', file=stderr)

        # --- sra datasets ---
        sra_jobs = []
        sra_reg = self.register.subregisters['sra']
        if len(sra_reg) > 0:
            downloadable = self.sra is not None
            if downloadable:
                sra_jobs.extend(sra.jobs_from_accessions(sra_reg, datadir, self.sra))
            else:
                print(f'SRA data cannot be downloaded because the sra-tools are absent from the system. Skipping {len(sra_reg)} datasets.', file=stderr)

        # --- wget files ---
        wget_jobs = []
        # TODO

        manager = JobManager(max_process=max_process)
        manager.start()

        # submit the jobs in an interleaved way
        while len(ncbi_jobs) + len(sra_jobs) + len(wget_jobs) > 0:
            if len(ncbi_jobs) > 0:
                manager.add_process(ncbi_jobs.pop(0))
            if len(sra_jobs) > 0:
                manager.add_process(sra_jobs.pop(0))
            if len(wget_jobs) > 0:
                manager.add_process(wget_jobs.pop(0))

        while manager.remaining_jobs() > 0:
            # print(manager)
            time.sleep(1)

        manager.stop()
        manager.join()


# -------------------- Utils downloads --------------------

def check_binaries(binary_names, bindir, download_function):
    """ Look for the binary presence and download it if not present
    :param: binary_name Array of searched binaries
    :param: bindir Folder where the binary has to be download if needed
    :param: download_function function triggered if the binary have to be downloaded. the bindir will be given as parameter. True is expected as return on success.

    :return: address of the binary. None if not present and not downloadable.
    """

    installed = {name:False for name in binary_names}
    binaries = {}

    for binary_name in binary_names:
        # Global install
        try:
            if subprocess.run(f'{binary_name} --version'.split(' ')) == 0:
                binaries[binary_name] = binary_name
                installed[binary_name] = True
                continue
        except FileNotFoundError:
            print(f'{binary_name} not installed on the system. Checking locally...')

        # Local install
        local_bin = path.join(bindir, binary_name)
        # Check if not already present
        if not path.isfile(local_bin):
            print(f'{binary_name} is not installed locally. Automatic installation will be tried...')
        else:
            binaries[binary_name] = local_bin
            installed[binary_name] = True

    # Return binaries if everything has been found
    if all(installed.values()):
        return binaries

    # Automatic download
    binpaths = download_function(bindir)
    if binpaths is None:
        return None

    for name, local_bin in binpaths.items():
    # Exec locally installed binary
        try:
            ret = subprocess.run(f'{local_bin} --version'.split(' '))
            if ret.returncode == 0:
                binaries[name] = local_bin
        except FileNotFoundError:
            print(f'{name} still not installed locally. An error has occured during the download process.')

    return binaries



