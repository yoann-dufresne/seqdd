from os import listdir, makedirs, path, remove
import platform
from shutil import rmtree, move
import subprocess
from sys import stderr
from threading import Lock
import time

from seqdd.downloaders.download import check_binary
from seqdd.utils.scheduler import CmdLineJob, FunctionJob



class SRA:

    def __init__(self, tmpdir, bindir):
        self.tmpdir = tmpdir
        self.bindir = bindir
        self.binaries = self.download_sra_toolkit()
        
        self.mutex = Lock()
        self.min_delay = 0.5
        self.last_sra_query = 0

    def is_ready(self):
        return self.binaries is not None
    
    def sra_delay_ready(self):
        """
        Checks if the minimum delay between SRA queries has passed.

        Returns:
            bool: True if the minimum delay has passed, False otherwise.
        """
        # Minimal delay between SRA queries (0.5s)
        locked = self.mutex.acquire(blocking=False)
        ready = False
        if locked:
            ready = time.time() - self.last_sra_query > self.min_delay
            if ready:
                self.last_sra_query = time.time()
            self.mutex.release()
        return ready
    
    def filter_valid_accessions(self, accessions):
        print('TODO: Validate sra accessions...')
        return accessions

    
    def jobs_from_accessions(self, accessions, datadir):
        jobs = []

        # Each dataset download is independant
        for acc in accessions:
            tmp_dir = path.join(self.tmpdir, acc)
            job_name = f'sra_{acc}'

            # Prefetch data
            cmd = f'{self.binaries["prefetch"]} --max-size u --output-directory {tmp_dir} {acc}'
            prefetch_job = CmdLineJob(cmd, can_start=self.sra_delay_ready, name=f'{job_name}_prefetch')

            # Split files
            accession_dir = path.join(tmp_dir, acc)
            cmd = f'{self.binaries["fasterq-dump"]} --split-3 --skip-technical --outdir {accession_dir} {accession_dir}'
            fasterqdump_job = CmdLineJob(cmd, parents=[prefetch_job], can_start=self.sra_delay_ready, name=f'{job_name}_fasterqdump')
            
            # Compress files
            cmd = f'gzip {path.join(accession_dir, "*.fastq")}'
            compress_job = CmdLineJob(cmd, parents=[fasterqdump_job], name=f'{job_name}_compress')

            # Move to datadir and clean tmpdir
            clean_job = FunctionJob(self.move_and_clean, func_args=(accession_dir, datadir, tmp_dir), parents=[compress_job], name=f'{job_name}_clean')

            # Set the jobs
            jobs.extend((prefetch_job, fasterqdump_job, compress_job, clean_job))

        return jobs


    def move_and_clean(self, accession_dir, outdir, tmpdir):
        # Enumarates all the files from the accession directory
        for filename in listdir(accession_dir):
            if filename.endswith('.gz'):
                move(path.join(accession_dir, filename), path.join(outdir, filename))

        # Clean the directory
        rmtree(tmpdir)
    
    def download_sra_toolkit(self):
        # Check if the system has the ncbi datasets cli
        prefetch_installed = check_binary('prefetch')
        fasterqdump_installed = check_binary('fasterq-dump')
        if prefetch_installed and fasterqdump_installed:
            return {
                'prefetch': 'prefetch',
                'fasterq-dump': 'fasterq-dump'
            }
        
        # Check if the software is locally installed
        prefetch_local_bin = path.abspath(path.join(self.bindir, 'prefetch'))
        fasterqdump_local_bin = path.abspath(path.join(self.bindir, 'fastq-dump'))
        prefetch_installed = check_binary(prefetch_local_bin)
        fasterqdump_installed = check_binary(fasterqdump_local_bin)
        if prefetch_installed and fasterqdump_installed:
            return {
                'prefetch': f'{prefetch_local_bin}',
                'fasterq-dump': f'{fasterqdump_local_bin}'
            }
        
        # Install the software
        return self.install_sratoolkit()

    def install_sratoolkit(self, version='3.1.1'):
        download_link = ''
        dirname = ''
        supported = True

        # Local install
        system = platform.system()
        if system == 'Linux':
            download_link = f'https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/{version}/sratoolkit.{version}-ubuntu64.tar.gz'
            dirname = f'sratoolkit.{version}-ubuntu64'
            tarname = f'{dirname}.tar.gz'
        else:
            supported = False

        # Message to potential system extensions
        if not supported:
            print('sratoolkit auto-install is not yet supported on your system. SRA downloader has not been installed... Also maybe we can include your system in the auto-installer. Please submit an issue on github with the following values:', file=stderr)
            print(f'system={system}\tplateform={platform.machine()}', file=stderr)
            return None

        # Download sra toolkit
        tmp_dir = path.abspath(self.tmpdir)
        makedirs(tmp_dir, exist_ok=True)
        tarpath = path.join(tmp_dir, tarname)

        cmd = f'curl -o {tarpath} {download_link}'
        ret = subprocess.run(cmd.split())

        if ret.returncode != 0:
            print('Impossible to automatically download sratoolkit. SRA downloader has not been installed...', file=stderr)
            return None

        # Uncompress the archive
        cmd = f'tar -xzf {tarpath} -C {self.bindir}'
        ret = subprocess.run(cmd.split())
        remove(tarpath)

        if ret.returncode != 0:
            print('Impossible to expand the sratoolkit tar.gz on your system.', file=stderr)
            return None

        # Create links to the bins
        prefetch_bin = path.abspath(path.join(self.bindir, dirname, 'bin', 'prefetch'))
        prefetch_ln = path.abspath(path.join(self.bindir, 'prefetch'))
        cmd = f'ln -s {prefetch_bin} {prefetch_ln}'
        ret = subprocess.run(cmd.split())
        if ret.returncode != 0:
            print(f'Impossible to create symbolic link {prefetch_ln}. SRA downloader has not been installed...', file=stderr)
            return None

        fasterqdump_bin = path.abspath(path.join(self.bindir, dirname, 'bin', 'fasterq-dump'))
        fasterqdump_ln = path.abspath(path.join(self.bindir, 'fasterq-dump'))
        cmd = f'ln -s {fasterqdump_bin} {fasterqdump_ln}'
        ret = subprocess.run(cmd.split())
        if ret.returncode != 0:
            print(f'Impossible to create symbolic link {fasterqdump_ln}. SRA downloader has not been installed...', file=stderr)
            return None

        return {
            'prefetch' : prefetch_ln,
            'fasterq-dump' : fasterqdump_ln
        }


# --- Cmds ---
# ./sratoolkit.3.1.1-ubuntu64/bin/prefetch --max-size u --output-directory outtest SRR000001
# ./sratoolkit.3.1.1-ubuntu64/bin/fasterq-dump --split-3 --skip-technical --outdir outtest outtest/SRR000001

