from seqdd.utils.scheduler import Job
from shutil import rmtree
from os import path, listdir, rename, remove
from os import makedirs
from sys import stderr
import platform
import subprocess
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
            self.ncbi = check_binaries(['datasets'], bindir, download_datasets_software)
        if len(self.register.subregisters['sra']) > 0:
            self.sra = check_binaries(['prefetch', 'fasterq-dump'], bindir, download_sra_toolkit)

        return self.ncbi, self.sra, wget

    def download_to(self, datadir):
        manager = JobManager(max_process=1)
        manager.start()

        # Creates the data directory if not existing
        makedirs(datadir, exist_ok=True)

        # --- ncbi genomes ---
        ncbi_reg = self.register.subregisters['ncbi']
        if len(ncbi_reg) > 0:
            downloadable = self.ncbi is not None
            if downloadable:
                manager.add_processes(ncbi_jobs_from_accessions(ncbi_reg, datadir, self.ncbi['datasets']))

            else:
                print(f'ncbi genomes cannot be downloaded because the datasets tool is absent from the system. Skipping {len(ncbi_reg)} datasets.', file=stderr)

        # --- sra datasets ---
        sra_reg = self.register.subregisters['sra']
        if len(sra_reg) > 0:
            downloadable = self.sra is not None
            if downloadable:
                manager.add_processes(sra_jobs_from_accessions(sra_reg, datadir, self.sra))
            else:
                print(f'SRA data cannot be downloaded because the sra-tools are absent from the system. Skipping {len(sra_reg)} datasets.', file=stderr)

        # --- wget files ---
        # TODO

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
    print('Download the ncbi datasets cli binnary...')
    cmd = f'wget {download_link} --directory-prefix={path.abspath(dest_dir)}'
    ret = subprocess.run(cmd.split())

    if ret.returncode == 0:
        binpath = path.join(dest_dir, 'datasets')

        ret = subprocess.run(f'chmod +x {binpath}'.split())
        if ret.returncode == 0:
            return {'datasets': binpath}
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
    unzip_job = CmdLineJob(f"unzip {download_file} -d {unzip_dir}", parents=[download_job])

    # Data download
    rehydrate_job = CmdLineJob(f"{datasets_bin} rehydrate --gzip --no-progressbar --directory {unzip_dir}", parents=[unzip_job])

    # Data reorganization
    reorg_job = FunctionJob(ncbi_clean, func_args=(download_file, unzip_dir, dest_dir), parents=[rehydrate_job])

    return download_job, unzip_job, rehydrate_job, reorg_job


def ncbi_clean(archive, unzip_dir, dest_dir):
    # Remove the downloaded archive
    remove(archive)

    # Remove subdirectories while moving their content
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


def sra_jobs_from_accessions(accessions, datadir, binaries):
    jobs = []

    # Each dataset download is independant
    for acc in accessions:
        # Prefetch data
        cmd = f'./{binaries["prefetch"]} --max-size u --output-directory {datadir} {acc}'
        prefetch_job = CmdLineJob(cmd)

        # Split files
        accession_dir = path.join(datadir, acc)
        cmd = f'{binaries["fasterq-dump"]} --split-3 --skip-technical --outdir {accession_dir} {accession_dir}'
        fasterqdump_job = CmdLineJob(cmd, parents=[prefetch_job])
        
        # Compress files
        cmd = f'gzip {path.join(accession_dir, "*.fastq")}'
        compress_job = CmdLineJob(cmd, parents=[fasterqdump_job])

        # Move to datadir and clean
        clean_job = FunctionJob(sra_move_and_clean, func_args=(accession_dir, datadir), parents=[compress_job])

        # Set the jobs
        jobs.extend((prefetch_job, fasterqdump_job, compress_job, clean_job))

    return jobs


def sra_move_and_clean(accession_dir, outdir):
    # Enumarates all the files from the accession directory
    for filename in listdir(accession_dir):
        if filename.endswith('.gz'):
            rename(path.join(accession_dir, filename), path.join(outdir, filename))

    # Clean the directory
    rmtree(accession_dir)

# --- Cmds ---
# ./sratoolkit.3.1.1-ubuntu64/bin/prefetch --max-size u --output-directory outtest SRR000001
# ./sratoolkit.3.1.1-ubuntu64/bin/fasterq-dump --split-3 --skip-technical --outdir outtest outtest/SRR000001



def download_sra_toolkit(dest_dir, version='3.1.1'):
    download_link = ''
    dirname = ''
    supported = True

    system = platform.system()
    if system == 'Linux':
        download_link = f'https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/{version}/sratoolkit.{version}-ubuntu64.tar.gz'
        dirname = f'sratoolkit.{version}-ubuntu64'
    elif system == 'Windows':
        print('Windows plateforms are not supported by seqdd.', file=stderr)
        exit(3)
    else:
        supported = False

    # Message to potential system extensions
    if not supported:
        print('sratoolkit auto-install is not yet supported on your system. SRA downloader has not been installed... Also maybe we can include your system in the auto-installer. Please submit an issue on github with the following values:', file=stderr)
        print(f'system={system}\tplateform={platform.machine()}', file=stderr)
        return None

    # Download sra toolkit
    cmd = f'wget {download_link} --directory-prefix={path.abspath(dest_dir)}'
    ret = subprocess.run(cmd.split())

    if ret.returncode != 0:
        print('Impossible to automatically download sratoolkit. SRA downloader has not been installed...', file=stderr)
        return None

    # Uncompress the archive
    archive_path = path.join(dest_dir, f'{dirname}.tar.gz')
    cmd = f'tar -xzf {archive_path} -C {dest_dir}'
    ret = subprocess.run(cmd.split())
    remove(archive_path)

    if ret.returncode != 0:
        print('Impossible to expand the sratoolkit tar.gz on your system.', file=stderr)
        return None

    # Create links to the bins
    prefetch_bin = path.join(path.abspath(dest_dir), dirname, 'bin', 'prefetch')
    prefetch_ln = path.join(dest_dir, 'prefetch')
    cmd = f'ln -s {prefetch_bin} {prefetch_ln}'
    ret = subprocess.run(cmd.split())
    if ret.returncode != 0:
        print(f'Impossible to create symbolic link {prefetch_ln}. SRA downloader has not been installed...', file=stderr)
        return None

    fasterqdump_bin = path.join(path.abspath(dest_dir), dirname, 'bin', 'fasterq-dump')
    fasterqdump_ln = path.join(dest_dir, 'fasterq-dump')
    cmd = f'ln -s {fasterqdump_bin} {fasterqdump_ln}'
    ret = subprocess.run(cmd.split())
    if ret.returncode != 0:
        print(f'Impossible to create symbolic link {fasterqdump_ln}. SRA downloader has not been installed...', file=stderr)
        return None

    return {
        'prefetch' : prefetch_ln,
        'fasterq-dump' : fasterqdump_ln
    }


