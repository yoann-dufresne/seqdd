from os import path
from urllib.parse import urlparse
import subprocess
from sys import stderr
from threading import Lock
import time

from seqdd.utils.scheduler import CmdLineJob


def jobs_from_accessions(urls, datadir):
    jobs = []

    for idx, url in enumerate(urls):
        # Get the filename from the server
        filename = get_filename(url)
        filepath = path.join(datadir, f'url{idx}_{filename}')

        # Make the download
        jobs.append(CmdLineJob(f'curl -o {filepath} "{url}"'))

    return jobs

def filter_valid_accessions(urls):
    # Verify schemes
    curl_schemes = set(('http', 'https', 'ftp'))

    valid_urls = set()
    for url in urls:
        scheme = url[:url.find(':')].lower()
        if scheme not in curl_schemes:
            print(f'WARNING: scheme {scheme} not supported.', file=stderr)
            print(f'url ignored: {url}', file=stderr)

        # Wait a delay to not DDOS servers
        wait_my_turn()
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


def get_filename(url):
    # Wait a delay to not DDOS servers
    wait_my_turn()
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


# Limiting query per second
query_lock = Lock()
min_delay = .5
last_query = 0

def wait_my_turn():
    global query_lock
    global min_delay
    global last_query

    query_lock.acquire()

    t = time.time()
    dt = t - last_query
    if dt < min_delay:
        time.sleep(min_delay - dt)

    last_query = t

    query_lock.release()
