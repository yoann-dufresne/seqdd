import logging
import abc
from threading import Lock
from ...utils.scheduler import Job


class DataType(metaclass=abc.ABCMeta):
    """
    Abstract Base Class of data DataSource
    """

    def __init__(self, tmpdir: str, bindir: str, logger: logging.Logger, min_delay: float = 0) -> None:
        """
        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        :param bindir: The binary directory path. Where the helper binaries tools are stored.
        :param logger: The logger object.
        """
        self.tmp_dir = tmpdir
        """The temporary directory path. Where the downloaded intermediate files are located."""
        self.bin_dir = bindir
        """The binary directory path. Where the helper binaries tools are stored."""
        self.logger = logger
        """The logger object for logging messages."""
        self.mutex = Lock()
        """A lock object for thread synchronization."""
        self.last_query = 0
        """The timestamp of the last query."""
        self.min_delay = min_delay
        """The minimum delay between queries in seconds."""


    # @abc.abstractmethod
    # def src_delay_ready(self) -> bool:
    #     """
    #     :return: True if the minimum delay has passed between queries, False otherwise.
    #     """
    #     pass


    @abc.abstractmethod
    def jobs_from_accessions(self, accessions: list[str], datadir: str) -> list[Job]:
        """
        Generates a list of jobs for downloading and processing datasets.

        :param accessions: A list of accessions for that source.
        :param datadir: The output directory path. Where the expected files will be located.
        :returns: A list of jobs for downloading and processing datasets.
        """
        pass