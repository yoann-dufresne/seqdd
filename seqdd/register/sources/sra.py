from os import listdir, makedirs, path, remove
import platform
from shutil import rmtree, move
import subprocess
from sys import stderr
from threading import Lock
import time

from seqdd.utils.download import check_binary
from seqdd.utils.scheduler import CmdLineJob, FunctionJob


naming = {
    'name': 'SRA',
    'key': 'sra',
    'classname': 'SRA'
}


class SRA:
    """
    The SRA class represents a data downloader for the Sequence Read Archive (SRA) database.

    Attributes:
        tmpdir (str): The temporary directory path.
        bindir (str): The binary directory path.
        logger: The logger object for logging messages.
        binaries (dict): A dictionary containing the paths to the SRA toolkit binaries.
        mutex: A lock object for thread synchronization.
        min_delay (float): The minimum delay between SRA queries in seconds.
        last_sra_query (float): The timestamp of the last SRA query.

    """
    
    def __init__(self, tmpdir, bindir, logger):
        """
        Initialize the SRA downloader object.

        Args:
            tmpdir (str): The temporary directory path.
            bindir (str): The binary directory path.
            logger: The logger object.
        """
        self.tmpdir = tmpdir
        self.bindir = bindir
        self.logger = logger
        self.binaries = self.download_sra_toolkit()
        
        self.mutex = Lock()
        self.min_delay = 0.5
        self.last_sra_query = 0

    def is_ready(self):
        """
        Checks if the SRA toolkit binaries are ready for use.

        Returns:
            bool: True if the binaries are ready, False otherwise.
        """
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
        """
        Filters the given list of SRA accessions and returns only the valid ones.

        Args:
            accessions (list): A list of SRA accessions.

        Returns:
            list: A list of valid SRA accessions.
        """
        # print('TODO: Validate sra accessions...')
        return accessions

    
    def jobs_from_accessions(self, accessions, datadir):
        """
        Generates a list of jobs for downloading and processing SRA datasets.

        Args:
            accessions (list): A list of SRA accessions.
            datadir (str): The output directory path.

        Returns:
            list: A list of jobs for downloading and processing SRA datasets.
        """
        jobs = []

        # Each dataset download is independent
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
        """
        Moves the downloaded files from the accession directory to the output directory and cleans up the temporary directory.

        Args:
            accession_dir (str): The directory path containing the downloaded files.
            outdir (str): The output directory path.
            tmpdir (str): The temporary directory path.
        """
        # Enumerate all the files from the accession directory
        for filename in listdir(accession_dir):
            if filename.endswith('.gz'):
                move(path.join(accession_dir, filename), path.join(outdir, filename))

        # Clean the directory
        rmtree(tmpdir)
    
    def download_sra_toolkit(self):
        """
        Downloads and installs the SRA toolkit if necessary, and returns the paths to the SRA toolkit binaries.

        Returns:
            dict: A dictionary containing the paths to the SRA toolkit binaries.
        """
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
        fasterqdump_local_bin = path.abspath(path.join(self.bindir, 'fasterq-dump'))
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
        """
        Downloads and installs the SRA toolkit with the specified version.

        Args:
            version (str): The version of the SRA toolkit to install. Default is '3.1.1'.

        Returns:
            dict: A dictionary containing the paths to the SRA toolkit binaries, or None if installation failed.
        """
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
            self.logger.critical(f'sratoolkit auto-install is not yet supported on your system. SRA downloader has not been installed... Also maybe we can include your system in the auto-installer. Please submit an issue on github with the following values:\nsystem={system}\tplateform={platform.machine()}')
            return None

        # Download sra toolkit
        tmp_dir = path.abspath(self.tmpdir)
        makedirs(tmp_dir, exist_ok=True)
        tarpath = path.join(tmp_dir, tarname)

        self.logger.info('Download the sratoolkit binnaries...')

        cmd = f'curl -o {tarpath} {download_link}'
        ret = subprocess.run(cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if ret.returncode != 0:
            self.logger.error('Impossible to automatically download sratoolkit. SRA downloader has not been installed...')
            return None

        # Uncompress the archive
        cmd = f'tar -xzf {tarpath} -C {self.bindir}'
        ret = subprocess.run(cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        remove(tarpath)

        if ret.returncode != 0:
            self.logger.error('Impossible to expand the sratoolkit tar.gz on your system.')
            return None

        # Create links to the bins
        prefetch_bin = path.abspath(path.join(self.bindir, dirname, 'bin', 'prefetch'))
        prefetch_ln = path.abspath(path.join(self.bindir, 'prefetch'))
        cmd = f'ln -s {prefetch_bin} {prefetch_ln}'
        ret = subprocess.run(cmd.split())
        if ret.returncode != 0:
            self.logger.error(f'Impossible to create symbolic link {prefetch_ln}. SRA downloader has not been installed...')
            return None

        fasterqdump_bin = path.abspath(path.join(self.bindir, dirname, 'bin', 'fasterq-dump'))
        fasterqdump_ln = path.abspath(path.join(self.bindir, 'fasterq-dump'))
        cmd = f'ln -s {fasterqdump_bin} {fasterqdump_ln}'
        ret = subprocess.run(cmd.split())
        if ret.returncode != 0:
            self.logger.error(f'Impossible to create symbolic link {fasterqdump_ln}. SRA downloader has not been installed...')
            return None
        
        self.logger.info(f'SRA downloader binaries installed at {self.bindir}')

        return {
            'prefetch' : prefetch_ln,
            'fasterq-dump' : fasterqdump_ln
        }
        

# --- Cmds ---
# ./sratoolkit.3.1.1-ubuntu64/bin/prefetch --max-size u --output-directory outtest SRR000001
# ./sratoolkit.3.1.1-ubuntu64/bin/fasterq-dump --split-3 --skip-technical --outdir outtest outtest/SRR000001

