from seqdd.utils.scheduler import Job
from shutil import rmtree
from os import path, listdir, rename
import subprocess
from sys import stderr
import platform
import time

from seqdd.utils.scheduler import JobManager, CmdLineJob, FunctionJob


# -------------------- Global download manager --------------------

class DownloadManager:

    def __init__(self, register, bindir='bin'):
        self.register = register
        ncbi, sra, wget = self.get_downloaders(bindir)
        print(ncbi)

    def get_downloaders(self, bindir):
        self.datasets = check_binary('datasets', bindir, download_datasets_software)
        sra = None
        wget = None

        return self.datasets, sra, wget

    def download_to(self, datadir):
        manager = JobManager(max_process=1)
        manager.start()

        # --- ncbi genomes ---
        ncbi_reg = self.register.subregisters['ncbi']
        if len(ncbi_reg) > 0:
            downloadable = self.datasets is not None
            if downloadable:
                manager.add_processes(ncbi_jobs_from_accessions(ncbi_reg, datadir, self.datasets))

            else:
                print(f'ncbi genomes cannot be downloaded because the datasets tool is absent from the system. Skipping {len(ncbi_reg)} datasets.', file=stderr)

        # --- sra datasets ---
        # TODO

        # --- wget files ---
        # TODO

        while manager.remaining_jobs() > 0:
            # print(manager)
            time.sleep(1)

        manager.stop()
        manager.join()


# -------------------- Utils downloads --------------------

def check_binary(binary_name, bindir, download_function):
    """ Look for the binary presence and download it if not present
    :param: binary_name Name of the binary
    :param: bindir Folder where the binary has to be download if needed
    :param: download_function function triggered if the binary have to be downloaded. the bindir will be given as parameter. True is expected as return on success.

    :return: address of the binary. None if not present and not downloadable.
    """

    # Global install
    try:
        if subprocess.run(f'{binary_name} --version'.split(' ')) == 0:
            return binary_name
    except FileNotFoundError:
        print(f'{binary_name} not installed on the system. Checking locally...')

    # Local install
    local_bin = path.join(bindir, binary_name)
    # Check if not already present
    if not path.isfile(local_bin):
        print(f'{binary_name} is not installed locally. Try local automatic installation...')
        # Automatic download
        binpath = download_function(bindir)
        # Failed download
        if binpath is None:
            return None

    # Exec locally installed binary
    try:
        ret = subprocess.run(f'{local_bin} --version'.split(' '))
        if ret.returncode == 0:
            return local_bin
    except FileNotFoundError:
        print(f'{binary_name} not installed locally. Try automatic local install...')

    return None


# -------------------- NCBI genome downloads --------------------

def download_datasets_software(dest_dir):
    download_link = ''
    supported = True

    system = platform.system()
    if system == 'Linux':
        cpu_type = platform.machine()
        if cpu_type in ['i386', 'i686', 'x86_64', 'x86', 'AMD64']:
            download_link = 'https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-amd64/datasets'
        elif cpu_type in ['aarch64_be', 'aarch64', 'armv8b', 'armv8l', 'arm']:
            download_link = 'https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-arm64/datasets'
        else:
            supported = False
    elif system == 'Windows':
        print('Windows plateforms are not supported by seqdd.', file=stderr)
        exit(3)
    else:
        supported = False

    # Message to potential system extensions
    if not supported:
        print('ncbi datasets auto-install is not yet supported on your system. Plese install ncbi datasets cli by yourself. Also maybe we can include your system in the auto-installer. Please submit an issue on github with the following values:', file=stderr)
        print(f'system={system}\tplateform={platform.machine()}', file=stderr)
        return None

    # Download datasets
    cmd = f'wget {download_link} --directory-prefix={path.abspath(dest_dir)}'
    ret = subprocess.run(cmd.split())

    if ret.returncode == 0:
        print('Download the ncbi datasets cli binnary...')
        binpath = path.join(dest_dir, 'datasets')

        ret = subprocess.run(f'chmod +x {binpath}'.split())
        if ret.returncode == 0:
            return binpath
        else:
            print(f'Impossible to change the exec rights for {binpath}. Automatic download of ncbi datasets cli is aborted. Please install it by yourself.', file=stderr)
    else:
        print('Impossible to automatically download ncbi datasets cli. Please install it by yourself.', file=stderr)

    return None


ncbi_joib_id = 0
def ncbi_jobs_from_accessions(accessions, dest_dir, datasets_bin):
    # Job name
    global ncbi_joib_id
    job_name = f'ncbi_job_{ncbi_joib_id}'
    ncbi_joib_id += 1

    # Download dehydrated job
    download_file = path.join(dest_dir, f'{job_name}.zip')
    download_job = CmdLineJob(f"{datasets_bin} download genome accession --dehydrated --filename {download_file} {' '.join(accessions)}")
    
    # Unzip Job
    unzip_dir = path.join(dest_dir, job_name)
    unzip_job = CmdLineJob(f"unzip {download_file} -d {job_name}", parents=[download_job])

    # Data download
    rehydrate_job = CmdLineJob(f"{datasets_bin} rehydrate --gzip --no-progressbar --directory {unzip_dir}", parents=[unzip_job])

    # Data reorganization
    reorg_job = FunctionJob(ncbi_clean, func_args=(unzip_dir, dest_dir), parents=[rehydrate_job])

    return download_job, unzip_job, rehydrate_job, reorg_job


def ncbi_clean(unzip_dir, dest_dir):
    data_dir = path.join(unzip_dir, "ncbi_dataset", "data")

    # Enumerated the downloaded files
    for subname in listdir(data_dir):
        subpath = path.join(data_dir, subname)
        # Looks only for datasets
        if path.isdir(subpath):
            # Looks only for datafiles
            for file in listdir(subpath):
                if file.endswith(".gz"):
                    filepath = path.join(subpath, file)
                    # Move the data to its final destination
                    rename(filepath, path.join(dest_dir, file))

    # Clean the download directory
    rmtree(unzip_dir)



# # -------------------- SRA downloads --------------------
