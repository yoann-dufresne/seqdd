import logging
import re

from seqdd.utils.scheduler import Job
from seqdd.register.sources.ena import ENA

from seqdd.register.data_type import DataContainer


class Sequences(DataContainer):
    """
    The Sequences class represents a data downloader for individual nucleotide sequence records
    (GenBank/EMBL/ENA accessions: genes, plasmids, organelles, viral genomes, WGS contigs…),
    downloaded as FASTA from the ENA browser API.
    """

    # Loose pre-filter for INSDC nucleotide accessions; the ENA API is the authoritative validator.
    accession_patterns = {
        'sequence': r'[A-Z]{1,2}[0-9]{5,8}(\.[0-9]+)?|[A-Z]{4,6}[0-9]{8,11}(\.[0-9]+)?'
    }

    @staticmethod
    def read_source(accession: str) -> str | None:
        for key, pattern in Sequences.accession_patterns.items():
            if re.fullmatch(pattern, accession):
                return key
        return None

    def __init__(self, source: ENA, logger: logging.Logger) -> None:
        """
        Initialize the Sequences data container.

        :param source: The ENA data source.
        :param logger: The logger object.
        """
        super().__init__(source)
        self.logger = logger

    def get_download_jobs(self, datadir: str) -> list[Job]:
        """
        Generates the list of jobs to download the registered sequence records.

        :param datadir: The output directory path where the expected files will be located.
        :return: A list of download jobs.
        """
        return self.source.jobs_from_sequences(self.data, datadir)

    def filter_valid(self, accessions: list[str]) -> list[str]:
        """
        Filters the given accessions: keep only well-formed INSDC sequence accessions that
        actually exist on the ENA servers.

        :param accessions: A list of accessions.
        :return: A list of valid sequence accessions.
        """
        valid_accessions = []
        for acc in accessions:
            if Sequences.read_source(acc) is not None:
                valid_accessions.append(acc)
            else:
                self.logger.warning(
                    f"Invalid accession format: {acc}. Expected an INSDC sequence accession (e.g. U00096.3)."
                )

        # ENA confirms (via the FASTA endpoint) which well-formed accessions actually exist.
        return self.source.valid_sequence_accessions(valid_accessions)
