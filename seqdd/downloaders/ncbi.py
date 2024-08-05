from os import listdir, makedirs, path, remove, rename
import platform
from shutil import rmtree
import subprocess
from sys import stderr
from threading import Lock
import time

from seqdd.utils.scheduler import CmdLineJob, FunctionJob


def filter_valid_accessions(accessions):
    print('TODO: Validate ncbi accessions...')
    return accessions


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
    
    # Prepare the bin directory
    dest_dir = path.abspath(dest_dir)
    makedirs(dest_dir, exist_ok=True)
    
    # Download...
    cmd = f'curl -o {path.join(dest_dir, "datasets")} {download_link}'
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
def jobs_from_accessions(accessions, dest_dir, datasets_bin):
    global ncbi_joib_id
    ncbi_mutex = Lock()
    last_ncbi_query = 0

    # Function to delay the queries to the server (avoid DDOS)
    def ncbi_delay_ready():
        # Minimal delay between ncbi queries (1s)
        min_delay = 1
        nonlocal last_ncbi_query
        locked = ncbi_mutex.acquire(blocking=False)
        ready = False
        if locked:
            # 5s since the last query ?
            ready = time.time() - last_ncbi_query > min_delay
            if ready:
                last_ncbi_query = time.time()
            ncbi_mutex.release()
        return ready

    accessions = list(accessions)
    all_jobs = []

    # Download accessions by batch of 5
    for idx in range(0, len(accessions), 5):
        # Job name
        job_name = f'ncbi_job_{ncbi_joib_id}'
        ncbi_joib_id += 1

        # Take the right slice of 5 accessions
        acc_slice = accessions[idx:idx+5]

        # Download dehydrated job
        download_file = path.join(dest_dir, f'{job_name}.zip')
        download_job = CmdLineJob(f"{datasets_bin} download genome accession --dehydrated --filename {download_file} {' '.join(acc_slice)}", can_start=ncbi_delay_ready)
        
        # Unzip Job
        unzip_dir = path.join(dest_dir, job_name)
        unzip_job = CmdLineJob(f"unzip {download_file} -d {unzip_dir}", parents=[download_job])

        # Data download
        rehydrate_job = CmdLineJob(f"{datasets_bin} rehydrate --gzip --no-progressbar --directory {unzip_dir}", parents=[unzip_job], can_start=ncbi_delay_ready)

        # Data reorganization
        reorg_job = FunctionJob(ncbi_clean, func_args=(download_file, unzip_dir, dest_dir), parents=[rehydrate_job])

        all_jobs.extend([download_job, unzip_job, rehydrate_job, reorg_job])

    return all_jobs


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