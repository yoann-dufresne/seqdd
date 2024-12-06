import logging
import re
import time

from seqdd.utils.scheduler import Job
from seqdd.register.sources.ena import ENA

from . import DataType


class ReadArchives(DataType):
    """
    The Logan class represents a data downloader for the Assemblies made on top of SRA.

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
    
    
    def __init__(self, tmpdir: str, bindir: str, logger: logging.Logger, unitigs: bool = False) -> None:
        """
        Initialize the ENA downloader object.

        Args:
            tmpdir (str): The temporary directory path.
            bindir (str): The binary directory path.
            logger: The logger object.
        """
        self.ena = ENA(tmpdir, bindir, logger)

    
    def wait_my_turn(self) -> None:
        """
        Waits for the minimum delay between ENA queries.
        WARNING: The function acquires the mutex lock. You must release it after using this function.
        """
        while not self.delay_ready():
            time.sleep(0.01)
        self.mutex.acquire()

    
    # --- ENA Job creations ---

    def jobs_from_accessions(self, accessions: list[str], datadir: str) -> list[Job]:
        """
        Generates a list of jobs for downloading and processing Logan datasets.

        Args:
            accessions (list): A list of Logan/SRA accessions.
            datadir (str): The output directory path.

        Returns:
            list: A list of jobs for downloading and processing Logan datasets.
        """
        return self.ena.jobs_from_accessions(accessions, datadir)

    
    def filter_valid_accessions(self, accessions: list[str]) -> list[str]:
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

        valid_accessions = self.ena.filter_valid_accessions(valid_accessions)

        return valid_accessions
    
