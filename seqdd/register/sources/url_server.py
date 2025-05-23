
from os import path
import subprocess
import time
from urllib.parse import urlparse
from seqdd.utils.scheduler import CmdLineJob, Job
from sources import DataSource

class UrlServer(DataSource):
    """
    A Source of download from urls
    """
    
    def __init__(self, tmpdir, bindir, logger, min_delay = 0):
        super().__init__(tmpdir, bindir, logger, min_delay)
        

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

            jobs.append(CmdLineJob(f'curl -o {filepath} "{url}"', can_start=self.url_delay_ready, name=job_name))

        return jobs


    def get_filename(self, url: str) -> str:
        """
        Get the filename from the server through the given URL.

        :param url: The URL.
        :return: The filename extracted from the URL.
        """
        while not self.url_delay_ready():
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

