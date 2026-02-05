import logging
from os import listdir, makedirs, path
import re
from shutil import rmtree, move
import subprocess

from seqdd.register.sources import DataSource
from ...utils.scheduler import Job, CmdLineJob, FunctionJob



class RefSeq(DataSource):
    """
    The RefSeq class represents a data downloader for the RefSeq database.
    """

    index_file = {
        "ftp": "ftp://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt"
    }

    # Regular expression for each type of ENA accession
    accession_patterns = {
        'Reference': r'GCF_[0-9]{9}\.[0-9]+'
    }


    def __init__(self, tmpdir: str, logger: logging.Logger) -> None:
        """
        Initialize the ENA downloader object.

        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        :param logger: The logger object.
        """
        super().__init__(tmpdir, logger, min_delay=0.35)
        self.index_ready = False


    def get_index(self) -> bool:
        """
        Downloads de RefSeq assembly summary file from NCBI FTP server.
        :returns: True if the index file was downloaded successfully, False otherwise.
        """
        if self.index_ready:
            return True

        index_url = self.index_file['ftp']
        index_path = path.join(self.tmp_dir, 'assembly_summary_refseq.txt')

        self.logger.info('Downloading RefSeq assembly summary file')

        # Download the index file
        cmd = f"curl -s {index_url} > {index_path}"
        curl_process = subprocess.run(
            cmd.split(' '),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        if curl_process.returncode != 0:
            self.logger.error(f'Error downloading RefSeq assembly summary file using command :\n{cmd}')
            self.logger.error('RefSeq index file could not be downloaded. RefSeq downloads will not be possible.')
            return False

        self.logger.info('RefSeq assembly summary file downloaded successfully')

        self.index = {}
        with open(index_path, 'r') as index_file:
            for line in index_file:
                split = line.strip().split('\t')
                acc, ftp = split[0], split[19]
                self.index[acc] = ftp

        return True

    # --- RefSeq Job creations ---

    def jobs_from_accessions(self, accessions: list[str], datadir: str) -> list[Job]:
        """
        Generates a list of jobs for downloading and processing RefSeq assemblies datasets.

        :param accessions: A list of RefSeq accessions.
        :param datadir: The output directory path. Where the expected files will be located.
        :returns: A list of jobs for downloading and processing RefSeq datasets.
        """
        jobs = []

        # Checking already downloaded accessions
        downloaded_accessions = frozenset(listdir(datadir))
        to_download = frozenset(accessions) - downloaded_accessions

        if len(to_download) == 0:
            self.logger.info(f'All {len(accessions)} RefSeq accessions are already downloaded')
            return jobs
    
        # Download the index file if not already done
        if self.get_index() is False:
            return jobs

        self.logger.info(f'Creating jobs for {len(accessions) - len(downloaded_accessions)} RefSeq accessions')

        # Parse the index file to get the FTP paths
        with open(path.join(self.tmp_dir, 'assembly_summary_refseq.txt'), 'r') as index_file:
            for line in index_file:
                split = line.strip().split('\t')
                acc, ftp_path = split[0], split[19]

                # Skip accessions that are not in the to_download set
                if acc not in to_download:
                    continue

                job_name = f'refseq_{acc}'
                # Reserve a temporary directory for the accession
                tmp_dir = path.join(self.tmp_dir, acc)
                if path.exists(tmp_dir):
                    rmtree(tmp_dir)
                makedirs(tmp_dir)

                # Creates a curl job for each URL
                wget_job =CmdLineJob(
                    command_line=f'wget -r -np -nH --cut-dirs=6 -e robots=off -P {tmp_dir} "{ftp_path}/"',
                    can_start = self.source_delay_ready,
                    name=f'{job_name}_wget'
                )
                jobs.append(wget_job)

                # Create a function job to move the files to the final directory
                jobs.append(FunctionJob(
                    func_to_run = self.move_and_clean,
                    func_args = (tmp_dir, path.join(datadir, acc)),
                    parents = [wget_job],
                    name=f'{job_name}_move'
                ))

        return jobs


    def move_and_clean(self, accession_dir: str, outdir: str) -> None:
        """
        Moves the downloaded files from the accession directory to the output directory and cleans
        up the temporary directory.

        :param accession_dir: The directory path containing the downloaded files.
        :param outdir: The output directory path. Where the expected files will be located.
        """
        move(accession_dir, outdir)

        # Clean the directory
        rmtree(accession_dir)

    # --- RefSeq accession validity ---

    def filter_valid(self, accessions: list[str]) -> list[str]:
        """
        Filters the given list of RefSeq accessions and returns only the valid ones.
        :param accessions: A list of RefSeq accessions.
        :returns: A list of valid RefSeq accessions.
        """
        # Download the index file if not already done
        if self.get_index() is False:
            self.logger.error('Cannot validate RefSeq accessions without index file')
            return []

        valid_accessions = []
        for acc in accessions:
            # Pattern validation
            acc_type = self.validate_accession(acc)
            if acc_type  == 'Invalid':
                continue

            # Index validation
            if acc not in self.index:
                self.logger.warning(f'Accession {acc} not found in RefSeq index')
                continue

            # Add to valid accessions
            valid_accessions.append(acc)

        return valid_accessions



    def validate_accession(self, accession: str) -> str:
        """
        Validates a given accession.

        :param accession: The accession to validate.
        :returns: The type of accession if it is valid, otherwise the literal 'Invalid'.
        """
        for accession_type, pattern in RefSeq.accession_patterns.items():
            if re.fullmatch(pattern, accession):
                return accession_type
        self.logger.warning(f'Invalid accession: {accession}')
        return 'Invalid'
