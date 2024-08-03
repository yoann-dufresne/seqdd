from os import path
from sys import stderr

from seqdd.utils.scheduler import CmdLineJob


def valid_accessions(urls):
    # Verify schemes
    valid_schemes = set(('http', 'https', 'ftp', 'dict', 'file', 'ftps', 'gopher', 'imap', 'imaps', 'ldap', 'ldaps', 'pop3', 'pop3s', 'rtmp', 'rtsp', 'scp', 'sftp', 'smb', 'smbs', 'smtp', 'smtps', 'telnet', 'tftp'))

    valid_urls = set()
    for url in urls:
        scheme = url[:url.find(':')].lower()
        if scheme in valid_schemes:
            valid_urls.add(url)
        else:
            print(f'WARNING: scheme {scheme} not supported.', file=stderr)
            print(f'url ignored: {url}', file=stderr)

    # Verify that a file is present through the URL
    print('TODO: Validate url accessions...')
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
