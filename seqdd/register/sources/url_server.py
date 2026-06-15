
from os import path
from urllib.parse import urlparse
from seqdd.utils.scheduler import FunctionJob, Job
from seqdd.utils import net
from seqdd.register.sources import DataSource

class UrlServer(DataSource):
    """
    A Source of download from urls
    """

    def __init__(self, tmpdir, logger, min_delay = 0, urlformater = None) -> None:
        super().__init__(tmpdir, logger, min_delay)
        self.urlformater = urlformater


    def set_urlformater(self, urlformater = None) -> None:
        """
        Set the URL formater function.

        :param urlformater: A function to format the URL.
        """
        if urlformater is not None and callable(urlformater):
            self.urlformater = urlformater
        else:
            self.logger.warning('No URL formater provided or it is not callable. Using default behavior.')
            self.urlformater = None


    def jobs_from_accessions(self, accessions: list[str], datadir: str) -> list[Job]:
        """
        Generates a list of jobs for downloading and processing datasets.

        :param accessions: A list of urls to download.
        :param datadir: The output directory path. Where the expected files will be located.
        :returns: A list of jobs for downloading and processing datasets.
        """
        jobs = []

        for idx, url in enumerate(accessions):
            filename = self.get_filename(url)
            filepath = path.join(datadir, f'url{idx}_{filename}')
            job_name = f'url_{filename}'

            jobs.append(FunctionJob(
                func_to_run=net.download_file,
                func_args=(url, filepath),
                can_start=self.source_delay_ready,
                name=job_name
            ))

        return jobs


    def get_filename(self, url: str) -> str:
        """
        Get the filename from the server through the given URL.

        :param url: The URL.
        :return: The filename extracted from the URL.
        """
        self.wait_my_turn()
        try:
            status, headers = net.http_head_headers(url)
        finally:
            self.end_my_turn()

        # Prefer the filename advertised by the server (Content-Disposition header)
        if status == 200:
            disposition = headers.get('Content-Disposition', '')
            if 'filename=' in disposition:
                return disposition.split('filename=')[1].strip().strip('"')

        # Fall back to the last path segment of the URL
        url_parsed = urlparse(url)
        return path.basename(url_parsed.path)

    def filter_valid(self, urls: list[str]) -> list[str]:
        """
        Filters the given list of urls and returns only the valid ones.

        :param urls: A list of URLs.
        :return: A list of valid URLs (HTTP 200 on a HEAD request).
        """
        valid_accessions = []

        for url in urls:
            # If a URL formater is provided, format the URL
            if self.urlformater:
                url = self.urlformater(url)
            # Check the URL is reachable (HEAD -> HTTP status code, robust to HTTP/1.1 and HTTP/2)
            self.wait_my_turn()
            try:
                status = net.http_status(url)
            finally:
                self.end_my_turn()

            # Check the response
            if status != 200:
                self.logger.warning(f'Not found: {url}')
                continue

            valid_accessions.append(url)

        return valid_accessions
