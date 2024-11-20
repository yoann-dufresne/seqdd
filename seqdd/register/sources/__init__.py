import logging
import abc
from threading import Lock
from ...utils.scheduler import Job


class Source(metaclass=abc.ABCMeta):


    def __init__(self, tmpdir: str, bindir: str, logger: logging.Logger) -> None:
        """
        Abstract Base Class of data Source

        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        :param bindir: The binary directory path. Where the helper binaries tools are stored.
        :param logger: The logger object.
        """
        self.tmp_dir = tmpdir
        self.bin_dir = bindir
        self.logger = logger
        self.mutex = Lock()


    @abc.abstractmethod
    def is_ready(self)-> bool:
        """
        :return: True if the source download software is ready to be used.
                 False otherwise.
        """
        pass

    @abc.abstractmethod
    def src_delay_ready(self) -> bool:
        """
        :return: True if the minimum delay has passed between queries, False otherwise.
        """
        pass

    @abc.abstractmethod
    def jobs_from_accessions(self, accessions: list[str], datadir: str) -> list[Job]:
        """
        Generates a list of jobs for downloading and processing datasets.

        :param accessions: A list of accessions for that source.
        :param datadir: The output directory path. Where the expected files will be located.
        :returns: A list of jobs for downloading and processing datasets.
        """
        pass



