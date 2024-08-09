from os import path, makedirs
from sys import stderr
import subprocess
import time

from seqdd.utils.scheduler import JobManager


# -------------------- Global download manager --------------------

class DownloadManager:

    def __init__(self, register, bindir='bin', tmpdir='/tmp'):
        self.register = register
        self.bindir = bindir
        self.tmpdir = tmpdir
        self.downloaders = {}
        
        self.init_downloaders()


    def init_downloaders(self):
        from seqdd.downloaders import url, ncbi, sra
        self.downloaders['ncbi'] = ncbi.NCBI(self.tmpdir, self.bindir)
        self.downloaders['sra'] = sra.SRA(self.tmpdir, self.bindir)
        self.downloaders['url'] = url.URL(self.tmpdir, self.bindir)


    def download_to(self, datadir, max_process=8):
        """
        Downloads datasets from different sources to the specified data directory.

        Args:
            datadir (str): The path to the data directory where the datasets will be downloaded.
            max_process (int, optional): The maximum number of processes to use for downloading. Defaults to 8.

        """
        # Creates the data directory if it doesn't exist
        makedirs(datadir, exist_ok=True)

        # Create a dictionary to store the jobs for each source
        jobs = {source: [] for source in self.register.subregisters}

        # Create the jobs for each source
        for source in self.register.subregisters:
            reg = self.register.subregisters[source]
            if len(reg) > 0:
                if source in self.downloaders:
                    downloader = self.downloaders[source]
                    if downloader.is_ready():
                        jobs[source] = downloader.jobs_from_accessions(reg, datadir)
                        print(f'{len(reg)} datasets from {source} will be downloaded.')
                    else:
                        print(f'{source} datasets cannot be downloaded because the downloader is not ready. Skipping {len(reg)} datasets.', file=stderr)

        # Create a JobManager instance
        manager = JobManager(max_process=max_process)
        manager.start()

        # Add jobs to the JobManager in an interleaved way.
        # Doing this will allow the JobManager to start jobs from different sources in parallel.
        idxs = {source: 0 for source in jobs}
        total_jobs = sum([len(jobs[source]) for source in jobs])
        while total_jobs > 0:
            for source in jobs:
                if idxs[source] < len(jobs[source]):
                    manager.add_process(jobs[source][idxs[source]])
                    idxs[source] += 1
                    total_jobs -= 1

        print(manager)

        # Wait for all jobs to complete
        while manager.remaining_jobs() > 0:
            time.sleep(1)

        # Stop and join the JobManager
        manager.stop()
        manager.join()


# -------------------- Utils downloads --------------------


def check_binary(path_to_bin):
    """ Check if the binary is present and executable
    :param: path_to_bin Path to the binary

    :return: True if the binary is present and executable. False otherwise.
    """
    try:
        cmd = f'{path_to_bin} --version'
        ret = subprocess.run(cmd.split(' '))
        return ret.returncode == 0
    except FileNotFoundError:
        return False
    


