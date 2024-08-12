from os import path
from urllib.parse import urlparse
import subprocess
from sys import stderr
from threading import Lock
import time

from seqdd.utils.scheduler import CmdLineJob


class URL:

    def __init__(self, tmpdir, bindir):
        self.tmpdir = tmpdir
        self.bindir = bindir
        # Limiting query per second
        self.query_lock = Lock()
        self.min_delay = .5
        self.last_query = 0

    def is_ready(self):
        return True

    def url_delay_ready(self):
        """
        Checks if the minimum delay between URL queries has passed.

        Returns:
            bool: True if the minimum delay has passed, False otherwise.
        """
        locked = self.query_lock.acquire()
        if locked:
            ready = time.time() - self.last_query > self.min_delay
            if ready:
                self.last_query = time.time()
            self.query_lock.release()
            return ready
        return False
    
    def remaining_time_before_next_query(self):
        return max(0, self.min_delay - (time.time() - self.last_query))

    
    def jobs_from_accessions(self, urls, datadir):
        jobs = []

        for idx, url in enumerate(urls):
            # Get the filename from the server
            filename = self.get_filename(url)
            filepath = path.join(datadir, f'url{idx}_{filename}')
            job_name = f'url_{filename}'

            # Make the download
            jobs.append(CmdLineJob(f'curl -o {filepath} "{url}"', can_start=self.url_delay_ready, name=job_name))

        return jobs
    
    def get_filename(self, url):
        while not self.url_delay_ready():
            time.sleep(self.remaining_time_before_next_query())

        # Verify that a file is present through the URL
        verif_cmd = f'curl -X GET url -I "{url}"'
        res = subprocess.run(verif_cmd, shell=True, capture_output=True, text=True)

        # curl to ask the filename
        filename = None
        if res.returncode == 0:
            # read server response
            for line in res.stdout.split('\n'):
                if line.startswith('HTTP'):
                    code = int(line.strip().split(' ')[1])
                    if code != 200:
                        break
                if 'filename=' in line:
                    filename = line.split("=")[1].strip().strip('"')
                    return filename

        # curl has failed
        if filename is None:
            url_parsed = urlparse(url)
            filename = path.basename(url_parsed.path)

        return filename

    def filter_valid_accessions(self, urls):
        # Verify schemes
        curl_schemes = set(('http', 'https', 'ftp'))

        valid_urls = set()
        for url in urls:
            scheme = url[:url.find(':')].lower()
            if scheme not in curl_schemes:
                print(f'WARNING: scheme {scheme} not supported.', file=stderr)
                print(f'url ignored: {url}', file=stderr)

            # Wait a delay to not DDOS servers
            while not self.url_delay_ready():
                time.sleep(self.remaining_time_before_next_query())
            # Verify that a file is present through the URL
            verif_cmd = f'curl -X GET url -I "{url}"'
            res = subprocess.run(verif_cmd, shell=True, capture_output=True, text=True)

            # curl fail
            if res.returncode != 0:
                continue

            # bad http answer
            for line in res.stdout.split('\n'):
                if line.startswith('HTTP'):
                    code = int(line.strip().split(' ')[1])
                    if code == 200:
                        valid_urls.add(url)
                    else:
                        print(f'ERROR: {url}\nCannot donwload from this url. Error code: {code}\nSkipping...', file=stderr)
                    break
        return valid_urls

