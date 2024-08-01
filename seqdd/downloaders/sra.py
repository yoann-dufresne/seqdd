from os import listdir, path, remove, rename
import platform
from shutil import rmtree
import subprocess
from sys import stderr
from threading import Lock
import time

from seqdd.utils.scheduler import CmdLineJob, FunctionJob


def jobs_from_accessions(accessions, datadir, binaries):
    jobs = []

    # Each dataset download is independant
    for acc in accessions:
        # Delay mechanism
        sra_mutex = Lock()
        last_sra_query = 0

        # Function to delay the queries to the server (avoid DDOS)
        def sra_delay_ready():
            # Minimal delay between ncbi queries (1s)
            min_delay = 1
            nonlocal last_sra_query
            locked = sra_mutex.acquire(blocking=False)
            ready = False
            if locked:
                # 5s since the last query ?
                ready = time.time() - last_sra_query > min_delay
                if ready:
                    last_sra_query = time.time()
                sra_mutex.release()
            return ready

        # Prefetch data
        cmd = f'./{binaries["prefetch"]} --max-size u --output-directory {datadir} {acc}'
        prefetch_job = CmdLineJob(cmd, can_start=sra_delay_ready)

        # Split files
        accession_dir = path.join(datadir, acc)
        cmd = f'{binaries["fasterq-dump"]} --split-3 --skip-technical --outdir {accession_dir} {accession_dir}'
        fasterqdump_job = CmdLineJob(cmd, parents=[prefetch_job], can_start=sra_delay_ready)
        
        # Compress files
        cmd = f'gzip {path.join(accession_dir, "*.fastq")}'
        compress_job = CmdLineJob(cmd, parents=[fasterqdump_job])

        # Move to datadir and clean
        clean_job = FunctionJob(move_and_clean, func_args=(accession_dir, datadir), parents=[compress_job])

        # Set the jobs
        jobs.extend((prefetch_job, fasterqdump_job, compress_job, clean_job))

    return jobs


def move_and_clean(accession_dir, outdir):
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


