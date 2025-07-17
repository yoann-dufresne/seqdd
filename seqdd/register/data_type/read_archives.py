import logging
import re
import time

from seqdd.utils.scheduler import Job
from seqdd.register.sources.ena import ENA

from seqdd.register.data_type import DataContainer


class ReadArchives(DataContainer):
    """
    The REadArchive class represents a data downloader for the raw reads datasets from SRA/ENA/DRA.

    Attributes:
        tmpdir (str): The temporary directory path.
        bindir (str): The binary directory path.
        logger: The logger object for logging messages.
        mutex: A lock object for thread synchronization.
        min_delay (float): The minimum delay between ENA queries in seconds.
        last_query (float): The timestamp of the last ENA query.

    """
    # Regular expression for each type of Reads accession
    accession_patterns = {
        'ENA': r'ERP[0-9]{6,}|ERS[0-9]{6,}|ERR[0-9]{6,}|ERX[0-9]{6,}|PRJE[A-Z][0-9]+|SAME[A-Z]?[0-9]+',
        'DRA': r'DRA[0-9]{6,}|DRS[0-9]{6,}|DRR[0-9]{6,}|DRX[0-9]{6,}|PRJD[A-Z][0-9]+|SAMD[A-Z]?[0-9]+',
        'SRA': r'SRP[0-9]{6,}|SRS[0-9]{6,}|SRR[0-9]{6,}|SRX[0-9]{6,}|PRJN[A-Z][0-9]+|SAMN[A-Z]?[0-9]+'
    }

    @staticmethod
    def read_source(accession: str) -> str | None:
        if re.match(ReadArchives.accession_patterns['ENA'], accession):
            return 'ENA'
        elif re.match(ReadArchives.accession_patterns['DRA'], accession):
            return 'DRA'
        elif re.match(ReadArchives.accession_patterns['SRA'], accession):
            return 'SRA'
        return None
    
    
    def __init__(self, source: ENA, logger: logging.Logger) -> None:
        """
        Initialize the ENA downloader object.

        Args:
            tmpdir (str): The temporary directory path.
            bindir (str): The binary directory path.
            logger: The logger object.
        """
        super().__init__(source)
        self.logger = logger

    
    # --- ENA Job creations ---

    def get_download_jobs(self, datadir: str) -> list[Job]:
        """
        Generates a list of jobs for downloading and processing Logan datasets.

        Args:
            accessions (list): A list of Logan/SRA accessions.
            datadir (str): The output directory path.

        Returns:
            list: A list of jobs for downloading and processing Logan datasets.
        """
        return self.source.jobs_from_accessions(self.data, datadir)

    
    def filter_valid(self, accessions: list[str]) -> list[str]:
        """
        Filters the given list of Logan/SRA accessions and returns only the valid ones.

        Args:
            accessions (list): A list of Logan/SRA accessions.

        Returns:
            list: A list of valid Logan accessions.
        """
        valid_accessions = []

        for acc in accessions:
            # Valid the accession format
            acc_source = ReadArchives.read_source(acc)
            if acc_source is not None:
                valid_accessions.append(acc)

        valid_accessions = self.source.filter_valid(valid_accessions)

        return valid_accessions
    
