import logging
import abc
from threading import Lock
from ...utils.scheduler import Job
import time


class DataSource(metaclass=abc.ABCMeta):
    """
    Abstract Base Class for data sources.
    A data source is a "server" where data can be downloaded.

    Attributes:
        tmpdir (str): The temporary directory path where to download data.
        bindir (str): The binary directory path where needed local binaries are present.
        logger: The logger object for logging messages.
        mutex: A lock object to synch data awaiting.
        min_delay (float): The minimum delay between server queries in seconds.
        last_query (float): The timestamp of the last server query.
    """

    def __init__(self, tmpdir: str, bindir: str, logger: logging.Logger, min_delay: float = 0) -> None:
        self.tmp_dir = tmpdir
        self.bin_dir = bindir
        self.logger = logger
        self.mutex = Lock()
        self.last_query = 0
        self.min_delay = min_delay


    @abc.abstractmethod
    def jobs_from_accessions(self, accessions: list[str], datadir: str) -> list[Job]:
        """
        Generates a list of jobs for downloading and processing datasets.

        :param accessions: A list of accessions to download.
        :param datadir: The output directory path. Where the expected files will be located.
        :returns: A list of jobs for downloading and processing datasets.
        """
        pass
    
    
    def set_delay(self, delay: float) -> None:
        """
        Sets the minimum delay between queries to the source.

        :param delay: The minimum delay in seconds.
        """
        self.min_delay = delay
        # self.logger.info(f'Setting minimum delay to {self.min_delay} seconds')


    def source_delay_ready(self) -> bool :
        """
        Checks if the minimum delay between queries has passed.

        :returns: True if the minimum delay has passed, False otherwise.
        """

        locked = self.mutex.acquire(blocking=False)
        ready = False
        if locked:
            ready = time.time() - self.last_query > self.min_delay
            if ready:
                self.last_query = time.time()
            self.mutex.release()
        return ready


    def wait_my_turn(self) -> Lock:
        """
        Waits for the minimum delay between queries. The operation lock a mutex and returns it. The user can call end_my_turn on the source or manually release the mutex.

        Returns:
            mutex (Lock): A locked mutex that has to be released before the next query.

        """
        while not self.source_delay_ready():
            time.sleep(0.01)
        self.mutex.acquire()

        return self.mutex


    def end_my_turn(self) -> None:
        self.last_query = time.time()
        self.mutex.release()