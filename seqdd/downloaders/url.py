from os import path
import subprocess
from sys import stderr

from seqdd.utils.scheduler import CmdLineJob


def valid_accessions(urls):
    # Verify schemes
    curl_schemes = set(('http', 'https', 'ftp'))

    valid_urls = set()
    for url in urls:
        scheme = url[:url.find(':')].lower()
        if scheme not in curl_schemes:
            print(f'WARNING: scheme {scheme} not supported.', file=stderr)
            print(f'url ignored: {url}', file=stderr)

        # Verify that a file is present through the URL
        verif_cmd = f'curl -X GET url -I "{url}"'
        res = subprocess.run(verif_cmd, shell=True, capture_output=True, text=True)

        # curl fail
        if res.returncode != 0:
            continue

        # bad http answer
        for line in res.stdout.split('\n'):
            if line.startswith(scheme[:4].upper()):
                code = int(line.strip().split(' ')[-1])
                if code == 200:
                    valid_urls.add(url)
                else:
                    print(f'ERROR: {url}\nCannot donwload from this url. Error code: {code}\nSkipping...', file=stderr)
                break
    return valid_urls

def jobs_from_accessions(urls, datadir, outfilenames={}):
    '''
    Get the jobs list from the url to download. Because the general urls do not have any meanings, seqdd do not do anything with the file. The name of the file will be either a user specified name or the last part of the url before the \'?\' (plus some numbers if the fix url part reapeat itself)
    '''
    jobs = []

    for idx, url in enumerate(urls):
        # Compute the output filename
        if url not in outfilenames:
            # Find the separation between url and variables
            url_end = url.find('?')
            url_end = len(filename) if url_end == -1 else url_end
            # Extract the last part of the url
            url_short = url[:url_end]
            name = url_short[url_short.rfind('/')+1:]
            if len(name) == 0 or name in outfilenames:
                name = f'url{idx}_{name}'
            outfilenames[url] = name
        filename = outfilenames[url]

        # Make the download
        jobs.append(CmdLineJob(f'curl -o {path.join(datadir, filename)} "{url}"'))

    return jobs
