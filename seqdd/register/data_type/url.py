import logging
from seqdd.register.data_type import DataContainer
from seqdd.register.sources.url_server import UrlServer

from seqdd.utils.scheduler import Job


class URL(DataContainer):
    """
    A class that represents an URL data container.
    """


    def __init__(self, source: UrlServer, logger: logging.Logger) -> None:
        super().__init__(source)
        self.logger = logger
        

    def get_download_jobs(self, datadir: str) -> list[Job]:
        """
        Create a list of jobs for downloading files from the URLs.
        
        :param datadir: The output directory path where the expected files will be located.
        :return: A list of jobs for downloading files.
        """
        return self.source.jobs_from_accessions(self.data)


    