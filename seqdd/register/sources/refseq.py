import logging
from os import listdir, makedirs, path
import re
from shutil import rmtree, move

from seqdd.register.sources import DataSource
from ...utils.scheduler import Job, FunctionJob
from ...utils import net
from ...errors import DownloadError


class RefSeq(DataSource):
    """
    The RefSeq class represents a data downloader for the NCBI RefSeq database.
    Assemblies (GCF accessions) are resolved through the RefSeq assembly summary
    index and downloaded from the NCBI FTP server.
    """

    index_file = {
        "ftp": "ftp://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt"
    }

    # Regular expression for each type of RefSeq accession
    accession_patterns = {
        'Reference': r'GCF_[0-9]{9}\.[0-9]+'
    }

    def __init__(self, tmpdir: str, logger: logging.Logger) -> None:
        """
        Initialize the RefSeq downloader object.

        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        :param logger: The logger object.
        """
        super().__init__(tmpdir, logger, min_delay=0.35)
        self.index_ready = False
        self.index = {}
        self.gca_index = {}

    def get_index(self) -> bool:
        """
        Downloads the RefSeq assembly summary file from the NCBI FTP server and parses it.

        :returns: True if the index file was downloaded and parsed successfully, False otherwise.
        """
        if self.index_ready:
            return True

        index_url = self.index_file['ftp']
        index_path = path.join(self.tmp_dir, 'assembly_summary_refseq.txt')
        makedirs(self.tmp_dir, exist_ok=True)

        self.logger.info('Downloading RefSeq assembly summary file')

        # Download the index file with the pure-Python network layer (FTP via ftplib)
        try:
            net.download_file(index_url, index_path, resume=False)
        except DownloadError as err:
            self.logger.error(f'Error downloading RefSeq assembly summary file from {index_url}: {err}')
            self.logger.error('RefSeq index file could not be downloaded. RefSeq downloads will not be possible.')
            return False

        self.logger.info('RefSeq assembly summary file downloaded successfully')

        # Parse the index: per assembly accession (column 1), keep its FTP path (column 20) and
        # its paired GenBank assembly (column 18). The file begins with '#' comment lines to skip.
        self.index = {}
        self.gca_index = {}
        with open(index_path, 'r') as index_file:
            for line in index_file:
                if line.startswith('#'):
                    continue
                split = line.rstrip('\n').split('\t')
                if len(split) <= 19:
                    continue
                self.index[split[0]] = split[19]
                self.gca_index[split[0]] = split[17]

        self.index_ready = True
        return True

    # --- RefSeq Job creations ---

    def jobs_from_accessions(self, accessions: list[str], datadir: str) -> list[Job]:
        """
        Generates a list of jobs for downloading and processing RefSeq assembly datasets.

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

        self.logger.info(f'Creating jobs for {len(to_download)} RefSeq accessions')

        # Create the download jobs from the index (accession -> FTP path)
        for acc in to_download:
            ftp_path = self.index.get(acc)
            if ftp_path is None:
                self.logger.warning(f'Accession {acc} not found in RefSeq index. Skipping.')
                continue

            job_name = f'refseq_{acc}'
            # Reserve a temporary directory for the accession
            tmp_dir = path.join(self.tmp_dir, acc)
            if path.exists(tmp_dir):
                rmtree(tmp_dir)
            makedirs(tmp_dir)

            # Recursively download the assembly directory from the NCBI FTP server (pure Python)
            download_job = FunctionJob(
                func_to_run=net.download_ftp_dir,
                func_args=(f'{ftp_path}/', tmp_dir),
                can_start=self.source_delay_ready,
                name=f'{job_name}_download'
            )
            jobs.append(download_job)

            # Move the downloaded files to the final directory once the download is done
            jobs.append(FunctionJob(
                func_to_run=move_and_clean,
                func_args=(tmp_dir, path.join(datadir, acc)),
                parents=[download_job],
                name=f'{job_name}_move'
            ))

        return jobs

    # --- RefSeq accession validity ---

    def filter_valid(self, accessions: list[str]) -> list[str]:
        """
        Filters the given list of RefSeq accessions and returns only the valid ones.

        :param accessions: A list of RefSeq accessions.
        :returns: A list of valid RefSeq accessions (correct format and present in the RefSeq index).
        """
        # Download the index file if not already done
        if self.get_index() is False:
            self.logger.error('Cannot validate RefSeq accessions without index file')
            return []

        valid_accessions = []
        for acc in accessions:
            # Pattern validation
            if self.validate_accession(acc) == 'Invalid':
                continue

            # Index validation
            if acc not in self.index:
                self.logger.warning(f'Accession {acc} not found in RefSeq index')
                continue

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

    def latest_genbank_equivalent(self, accession: str) -> str | None:
        """
        Resolves the most recent GenBank (GCA) assembly equivalent to a RefSeq (GCF) accession,
        as served by ENA (sovereign European servers). Falls back to the GenBank assembly paired
        in the RefSeq index if ENA cannot be queried.

        :param accession: A RefSeq (GCF) accession.
        :returns: The most recent equivalent GCA accession, or None if it cannot be resolved.
        """
        # A GCF assembly and its GenBank counterpart share the same 9-digit core.
        core = accession.split('_')[-1].split('.')[0]
        url = f'https://www.ebi.ac.uk/ena/browser/api/xml/GCA_{core}'

        self.wait_my_turn()
        try:
            response = net.http_get_text(url)
        except DownloadError:
            response = None
        finally:
            self.end_my_turn()

        if response is not None:
            matches = re.findall(r'accession="(GCA_[0-9]+)\.([0-9]+)"', response)
            if matches:
                digits, version = max(matches, key=lambda m: int(m[1]))
                return f'{digits}.{version}'

        # Fallback: the GenBank assembly paired in the RefSeq index
        paired = self.gca_index.get(accession)
        if paired and paired.lower() != 'na':
            return paired
        return None


def move_and_clean(accession_dir: str, outdir: str) -> None:
    """
    Moves the downloaded files from the accession directory to the output directory.

    Defined at module level (not as a method) so it stays picklable as a ``FunctionJob`` target
    under the ``spawn`` multiprocessing start method (Windows/macOS).

    :param accession_dir: The directory path containing the downloaded files.
    :param outdir: The output directory path. Where the expected files will be located.
    """
    # shutil.move relocates the whole tree and removes the source directory,
    # so no extra cleanup is needed afterwards.
    move(accession_dir, outdir)
