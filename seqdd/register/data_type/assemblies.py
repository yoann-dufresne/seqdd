import logging
import re
import time

from seqdd.utils.scheduler import Job
from seqdd.register.sources.ena import ENA

from seqdd.register.data_type import DataContainer


class Assemblies(DataContainer):
    """
    The Assemblies class represents a data downloader for the Assemblies made on top of SRA.

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
        'RefSeq': r'GCF_\d{9}\.\d+',
        'assembly': r'GCA_\d{9}\.\d+'
    }
    
    @staticmethod
    def read_source(accession: str) -> str | None:
        for key, pattern in Assemblies.accession_patterns.items():
            if re.match(pattern, accession):
                return key
        return None
    
    
    def __init__(self, source: ENA) -> None:
        """
        Initialize the ENA downloader object.

        Args:
            tmpdir (str): The temporary directory path.
            bindir (str): The binary directory path.
            logger: The logger object.
        """
        super().__init__(source)

    
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
        return self.source.jobs_from_accessions(accessions, datadir)

    
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
            acc_source = Assemblies.read_source(acc)
            if acc_source is not None:
                valid_accessions.append(acc)

        valid_accessions = self.source.filter_valid_accessions(valid_accessions)

        return valid_accessions
    
