
from os import path
import subprocess
import time
from urllib.parse import urlparse
from seqdd.utils.scheduler import CmdLineJob, Job
from seqdd.register.sources import DataSource

class UrlServer(DataSource):
    """
    A Source of download from urls
    """
    
    def __init__(self, tmpdir, bindir, logger, min_delay = 0, urlformater = None) -> None:
        super().__init__(tmpdir, bindir, logger, min_delay)
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

            jobs.append(CmdLineJob(f'curl -o {filepath} "{url}"', can_start=self.source_delay_ready, name=job_name))

        return jobs


    def get_filename(self, url: str) -> str:
        """
        Get the filename from the server through the given URL.

        :param url: The URL.
        :return: The filename extracted from the URL.
        """
        self.wait_my_turn()

        verif_cmd = f'curl -X GET url -I "{url}"'
        res = subprocess.run(verif_cmd, shell=True, capture_output=True, text=True)
        
        self.end_my_turn()

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
    
    def filter_valid(self, urls: list[str]) -> list[str]:
        """
        Filters the given list of urls and returns only the valid ones.

        :param accessions: A list of URLs.
        :return: A list of valid URLs.
        """
        valid_accessions = []

        for url in urls:
            print(f'Checking URL: {url}')
            # If a URL formater is provided, format the URL
            if self.urlformater:
                url = self.urlformater(url)
            # Check if the accession is valid
            self.wait_my_turn()
            response = subprocess.run(['curl', '-I', url], capture_output=True)
            self.end_my_turn()
            
            # Check the response
            if response.returncode != 0:
                self.logger.error(f'Error querying: {url}\nAnswer: {response.stderr.decode()}')
                continue
            elif not response.stdout.decode().startswith('HTTP/1.1 200'):
                self.logger.warning(f'Not found: {url}')
                continue

            valid_accessions.append(url)

        return valid_accessions

