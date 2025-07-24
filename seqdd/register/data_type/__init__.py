from seqdd.register.sources import DataSource
from seqdd.utils.scheduler import Job
from abc import abstractmethod


class DataContainer:
    def __init__(self, source: DataSource) -> None:
        """
        Initialize the DataType object with a data source.

        :param source: The data source object.
        """
        self.source = source
        self.data = set()
        
        
    def add_data(self, data: list[str]) -> None:
        """
        Add data to the container.
        
        :param data: A list of data items to be added.
        """
        self.data |= set(data)
        
    def remove_data(self, data: list[str]) -> None:
        """
        Remove data from the container.
        
        :param data: A list of data items to be removed.
        """
        self.data -= set(data)
        
        
    def __len__(self) -> int:
        """
        Returns the number of items in the data container.
        
        :return: The number of items in the data container.
        """
        return len(self.data)
        
    @abstractmethod
    def get_download_jobs(self, datadir: str) -> list[Job]:
        """
        Abstract method to get download jobs for the data source.
        
        :param datadir: The output directory path where the expected files will be located.
        :return: A list of download jobs.
        """
        pass