import logging
import re
import subprocess
import time
from os import listdir, makedirs, path
from shutil import rmtree, move

from seqdd.register.data_type import DataContainer
from seqdd.utils.scheduler import Job, FunctionJob
from seqdd.register.sources.url_server import UrlServer


class Logan(DataContainer):
    """
    The Logan class represents a data downloader for the Assemblies made on top of SRA.
    """

    # 'SRR[0-9]{6,}'


    def __init__(self, source: UrlServer, unitigs: bool = False) -> None:
        """
        """
        super().__init__(source)
        self.source.set_delay(.35)
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

    def get_download_jobs(self, datadir: str) -> list[Job]:
        """
        Generates a list of jobs for downloading and processing Logan datasets.

        :param datadir: The output directory path.
        :return: A list of jobs for downloading and processing Logan datasets.
        """
        jobs = []
        accessions = self.data

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
            tmp_dir = path.join(self.tmp_dir, acc_dirname)
            if path.exists(tmp_dir):
                rmtree(tmp_dir)
            makedirs(tmp_dir)

            print(acc)
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
            url_jobs = self.source.jobs_from_accessions([url], tmp_dir)
            for job in url_jobs:
                if job.name.startswith('url_'):
                    # Rename the job to the accession name
                    job.name = f'{job_name}_download'
            jobs.extend(url_jobs)

            # Create a function job to move the files to the final directory
            jobs.append(FunctionJob(
                func_to_run = self.move_and_clean,
                func_args = (tmp_dir, datadir),
                parents = url_jobs,
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
            
            is_valid = len(self.url_server.filter_valid([url])) == 1
            if not is_valid:
                self.logger.warning(f'Invalid Logan/SRA accession: {acc}')
            else:
                acc = f"{acc}_{'unitigs' if self.unitigs else 'contigs'}"
                valid_accessions.append(acc)

        return valid_accessions
