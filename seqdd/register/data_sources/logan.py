import logging
import re
import subprocess
import time
from os import listdir, makedirs, path
from shutil import rmtree, move
from threading import Lock

from ...utils.scheduler import Job, CmdLineJob, FunctionJob
from . import DataSource

naming = {
    'name': 'Logan',
    'key': 'logan',
    'classname': 'Logan',
}


class Logan(DataSource):
    """
    The Logan class represents a data downloader for the Assemblies made on top of SRA.
    """

    # 'SRR[0-9]{6,}'
    
    
    def __init__(self, tmpdir: str, bindir: str, logger: logging.Logger, unitigs: bool = False) -> None:
        """
        Initialize the ENA downloader object.

        :param tmpdir: The temporary directory path.
        :param bindir: The binary directory path.
        :param logger: The logger object.
        :param unitigs:
        """
        super().__init__(tmpdir, bindir, logger, min_delay=0.35)

        self.unitigs = unitigs


    def set_option(self, option: str, value: str) -> None:
        """
        Sets an option for the downloader.

        :param option: The option name.
        :param value: The option value.
        """
        if option == 'unitigs':
            self.unitigs = value == 'True'
        else:
            self.logger.warning(f'Unknown option: {option}')


    def is_ready(self) -> bool:
        """
        No binaries, always ready.
        """
        return True


    def src_delay_ready(self) -> bool :
        """
        Checks if the minimum delay between queries has passed.

        :return: True if the minimum delay has passed, False otherwise.
        """
        # Minimal delay between Logan queries (0.35s)
        locked = self.mutex.acquire(blocking=False)
        ready = False
        if locked:
            ready = time.time() - self.last_query > self.min_delay
            if ready:
                self.last_query = time.time()
            self.mutex.release()
        return ready


    def wait_my_turn(self) -> None:
        """
        Waits for the minimum delay between ENA queries.

        .. warning:: The function acquires the mutex lock. You must release it after using this function.

        """
        while not self.src_delay_ready():
            time.sleep(0.01)
        self.mutex.acquire()

    
    # --- ENA Job creations ---

    def jobs_from_accessions(self, accessions: list[str], datadir: str) -> list[Job]:
        """
        Generates a list of jobs for downloading and processing Logan datasets.

        :param accessions: A list of Logan/SRA accessions.
        :param datadir: The output directory path.
        :return: A list of jobs for downloading and processing Logan datasets.
        """
        jobs = []

        # Checking already downloaded accessions
        downloaded_accessions = frozenset(listdir(datadir))
        
        self.logger.info(f'Creating jobs for {len(accessions) - len(downloaded_accessions)} Logan/SRA accessions')

        # Each dataset download is independent
        for acc in accessions:
            acc_dirname = f'logan_{acc}'
            # Skip already downloaded accessions
            if acc_dirname in downloaded_accessions:
                continue

            job_name = acc_dirname
            # Create a temporary directory for the accession
            tmp_dir = path.join(self.tmpdir, acc_dirname)
            if path.exists(tmp_dir):
                rmtree(tmp_dir)
            makedirs(tmp_dir)

            acc, type = acc.split('_')

            # Get the file name from the URL
            filename = None
            url = None
            if type == 'unitigs':
                filename = f'{acc}.unitigs.fa.zst'
                url = f'https://s3.amazonaws.com/logan-pub/u/{acc}/{filename}'
            else:
                filename = f'{acc}.contigs.fa.zst'
                url = f'https://s3.amazonaws.com/logan-pub/c/{acc}/{filename}'
            # Create the output file path
            output_file = path.join(tmp_dir, filename)
            # Create the command line job
            jobs.append(CmdLineJob(
                command_line=f'curl -s -o {output_file} "{url}"',
                can_start = self.src_delay_ready,
                name=f'{job_name}_download'
            ))
            
            # Create a function job to move the files to the final directory
            jobs.append(FunctionJob(
                func_to_run = self.move_and_clean,
                func_args = (tmp_dir, datadir),
                parents = [jobs[-1]],
                name=f'{job_name}_move'
            ))

        return jobs
    
    
    def move_and_clean(self, accession_dir: str, outdir: str) -> None:
        """
        Moves the downloaded files from the accession directory to the output directory and cleans up the temporary directory.

        :param accession_dir: The directory path containing the downloaded files.
        :param outdir: The output directory path.
        """
        acc_dirname = path.basename(accession_dir)
        dest_dir = path.join(outdir, acc_dirname)

        move(accession_dir, dest_dir)

    
    def filter_valid_accessions(self, accessions: list[str]) -> list[str]:
        """
        Filters the given list of Logan/SRA accessions and returns only the valid ones.

        :param accessions: A list of Logan/SRA accessions.
        :return: A list of valid Logan accessions.
        """
        valid_accessions = []

        for acc in accessions:
            # Check if the accession is valid
            string_valid = re.fullmatch('SRR[0-9]{6,}', acc)
            if not string_valid:
                self.logger.warning(f'Invalid Logan/SRA accession: {acc}')
                continue

            # Check if the accession is present on the Amazon S3 bucket
            url = None
            if self.unitigs:
                url = f'https://s3.amazonaws.com/logan-pub/u/{acc}/{acc}.unitigs.fa.zst'
            else:
                url = f'https://s3.amazonaws.com/logan-pub/c/{acc}/{acc}.contigs.fa.zst'
            response = subprocess.run(['curl', '-I', url], capture_output=True)
            if response.returncode != 0:
                self.logger.error(f'Error querying Logan/SRA\nQuery: {url}\nAnswer: {response.stderr.decode()}')
                continue
            elif not response.stdout.decode().startswith('HTTP/1.1 200'):
                self.logger.warning(f'Contigs of the accession not found on the Amazon S3 bucket: {acc}')
                continue
            
            acc = f"{acc}_{'unitigs' if self.unitigs else 'contigs'}"
            valid_accessions.append(acc)

        return valid_accessions
