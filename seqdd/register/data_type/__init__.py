from os import path
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
    
    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        return self.data == other.data

    def downloaded_accessions(self, datadir: str) -> set[str] | None:
        """
        Return the subset of this container's accessions already present in datadir.

        The default implementation matches the ``<datadir>/<accession>/`` layout used by the
        accession-keyed sources (ENA, RefSeq). Containers whose source stores files in a flat
        layout (url, logan) cannot resolve presence per accession and override this to return None.

        :param datadir: The data directory to inspect.
        :return: The set of accessions found on disk, or None if presence is not tracked per accession.
        """
        return {acc for acc in self.data if path.isdir(path.join(datadir, acc))}

    @abstractmethod
    def get_download_jobs(self, datadir: str) -> list[Job]:
        """
        Abstract method to get download jobs for the data source.
        
        :param datadir: The output directory path where the expected files will be located.
        :return: A list of download jobs.
        """
        pass