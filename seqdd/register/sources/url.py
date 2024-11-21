import logging
import subprocess
import time
from os import path
from collections.abc import Iterable
from urllib.parse import urlparse

from ...utils.scheduler import CmdLineJob
from . import Source

naming = {
    'name': 'url',
    'key': 'url',
    'classname': 'URL'
}

class URL(Source):
    """
    A class that represents a URL downloader.
    """


    def __init__(self, tmpdir: str, bindir: str, logger: logging.Logger) -> None:
        super().__init__(tmpdir, bindir, logger, min_delay=0.5)


    def is_ready(self) -> bool:
        """
        Check if the URL downloader is ready.

        :return: True if the URL downloader is ready, False otherwise.
        """
        return True


    def src_delay_ready(self) -> bool:
        """
        Checks if the minimum delay between URL queries has passed.

        :return: True if the minimum delay has passed, False otherwise.
        """
        locked = self.mutex.acquire()
        if locked:
            ready = time.time() - self.last_query > self.min_delay
            if ready:
                self.last_query = time.time()
            self.mutex.release()
            return ready
        return False


    def remaining_time_before_next_query(self) -> float:
        """
        Calculates the remaining time before the next URL query can be made.

        :return: The remaining time in seconds.
        """
        return max(0, self.min_delay - (time.time() - self.last_query))

    
    def jobs_from_accessions(self, urls: list[str], datadir: str) -> list[CmdLineJob]:
        """
        Create a list of jobs for downloading files from the given URLs.

        :param urls: A list of URLs.
        :param datadir: The directory path to save the downloaded files.
        :return: A list of jobs for downloading files.
        """
        jobs = []

        for idx, url in enumerate(urls):
            filename = self.get_filename(url)
            filepath = path.join(datadir, f'url{idx}_{filename}')
            job_name = f'url_{filename}'

            jobs.append(CmdLineJob(f'curl -o {filepath} "{url}"', can_start=self.src_delay_ready, name=job_name))

        return jobs


    def get_filename(self, url: str) -> str:
        """
        Get the filename from the server through the given URL.

        :param url: The URL.
        :return: The filename extracted from the URL.
        """
        while not self.src_delay_ready():
            time.sleep(self.remaining_time_before_next_query())

        verif_cmd = f'curl -X GET url -I "{url}"'
        res = subprocess.run(verif_cmd, shell=True, capture_output=True, text=True)

        filename = None
        if res.returncode == 0:
            for line in res.stdout.split('\n'):
                if line.startswith('HTTP'):
                    code = int(line.strip().split(' ')[1])
                    if code != 200:
                        break
                if 'filename=' in line:
                    filename = line.split("=")[1].strip().strip('"')
                    return filename

        if filename is None:
            url_parsed = urlparse(url)
            filename = path.basename(url_parsed.path)

        return filename


    def filter_valid_accessions(self, urls: Iterable[str]):
        """
        Filter out invalid URLs and return a set of valid URLs.

        :param urls: A list of URLs.
        :return: A set of valid URLs.
        """
        curl_schemes = {'http', 'https', 'ftp'}

        print("Filtering URLs...")
        valid_urls = set()
        for url in urls:
            scheme = url[:url.find(':')].lower()
            if scheme not in curl_schemes:
                self.logger.warning(f'WARNING: scheme {scheme} not supported.\nurl ignored: {url}')

            while not self.src_delay_ready():
                time.sleep(self.remaining_time_before_next_query())

            verif_cmd = f'curl -X GET url -I "{url}"'
            res = subprocess.run(verif_cmd, shell=True, capture_output=True, text=True)

            if res.returncode != 0:
                continue

            content_length = 0
            added = False
            failed = False

            for line in res.stdout.split('\n'):
                if line.startswith('HTTP'):
                    code = int(line.strip().split(' ')[1])
                    if code == 200:
                        valid_urls.add(url)
                        added = True
                    else:
                        failed = True
                        self.logger.error(f'{url}\nCannot download from this URL. Error code: {code}\nSkipping...')
                    break
                elif line.startswith('Content-Length'):
                    content_length = int(line.split(' ')[1])
                    break

            # Add the URL if it has a content length, even if there is no HTTP code
            if not failed and not added and content_length > 0:
                valid_urls.add(url)

        return valid_urls
