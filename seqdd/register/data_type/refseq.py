import logging
import re

from seqdd.utils.scheduler import Job
from seqdd.register.sources.refseq import RefSeq

from seqdd.register.data_type import DataContainer


class Refseq(DataContainer):
    """
    The Refseq class represents a data downloader for RefSeq assemblies (GCF accessions),
    downloaded from the NCBI FTP server through the RefSeq assembly summary index.
    """

    # Regular expression for each type of RefSeq accession
    accession_patterns = {
        'refseq': r'GCF_\d{9}\.\d+'
    }

    @staticmethod
    def read_source(accession: str) -> str | None:
        for key, pattern in Refseq.accession_patterns.items():
            if re.match(pattern, accession):
                return key
        return None

    def __init__(self, source: RefSeq, logger: logging.Logger) -> None:
        """
        Initialize the RefSeq data container.

        :param source: The RefSeq data source.
        :param logger: The logger object.
        """
        super().__init__(source)
        self.logger = logger

    def get_download_jobs(self, datadir: str) -> list[Job]:
        """
        Generates the list of jobs to download the registered RefSeq assemblies.

        :param datadir: The output directory path where the expected files will be located.
        :return: A list of download jobs.
        """
        return self.source.jobs_from_accessions(self.data, datadir)

    def filter_valid(self, accessions: list[str]) -> list[str]:
        """
        Filters the given accessions, keeping only well-formed RefSeq (GCF) accessions
        that are present on the RefSeq servers.

        :param accessions: A list of accessions.
        :return: A list of valid RefSeq accessions.
        """
        valid_accessions = []
        for acc in accessions:
            if Refseq.read_source(acc) is not None:
                valid_accessions.append(acc)
            else:
                self.logger.warning(
                    f"Invalid accession format: {acc}. Expected format: {', '.join(Refseq.accession_patterns.values())}"
                )

        valid_accessions = self.source.filter_valid(valid_accessions)
        self.announce_genbank_equivalents(valid_accessions)
        return valid_accessions

    def announce_genbank_equivalents(self, accessions: list[str]) -> None:
        """
        Prints (on stdout) a notice that each RefSeq (GCF) accession is downloaded from NCBI and
        that a sovereign GenBank (GCA) equivalent is available from ENA, suggesting the most recent
        version so the user can choose the European source instead.

        :param accessions: The valid RefSeq accessions being registered.
        """
        for acc in accessions:
            gca = self.source.latest_genbank_equivalent(acc)
            if gca is None:
                continue
            print(
                f"[refseq] {acc} is downloaded from the NCBI servers. "
                f"A GenBank-equivalent assembly is available from ENA (sovereign European servers): "
                f"{gca} (most recent version). "
                f"To use it instead: seqdd add -t assemblies -a {gca}"
            )
