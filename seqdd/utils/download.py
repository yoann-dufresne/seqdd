import logging
from os import path, makedirs
from shutil import rmtree
import subprocess
import time

from seqdd.utils.scheduler import JobManager
from ..register.reg_manager import Register
from ..register.datatype_manager import DataTypeManager

# -------------------- Global download manager --------------------

class DownloadManager:
    """
    Class to handle download from different source
    """

    def __init__(self, register: Register, datatype_manager: DataTypeManager, logger: logging.Logger) -> None:
        """

        :param register:
        :param src_manager:
        :param logger: The logger object.
        :param bindir: The binary directory path. Where the helper binaries tools are stored.
        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        """
        # The sources register
        self.register = register
        # The binary directory path. Where the helper binaries tools are stored.
        self.bin_dir = datatype_manager.bindir
        # The temporary directory path. Where the downloaded intermediate files are located.
        self.tmp_dir = datatype_manager.tmpdir
        # The datatype manager to handle different data types
        # This is used to get the source manager for each data type
        # and to handle the download of datasets from different sources.
        self.datatype_manager = datatype_manager
        self.logger = self.register.logger


    def download_to(self, datadir, logdir, max_process=8) -> None:
        """
        Downloads datasets from different sources to the specified data directory.

        :param datadir: The path to the data directory where the datasets will be downloaded.
        :param logdir: The path to the log directory where the log files will be stored.
        :param max_process: The maximum number of processes to use for downloading. Defaults to 8.
        """
        # Creates the tmp and data directory if it doesn't exist
        makedirs(datadir, exist_ok=True)
        if logdir is not None and path.exists(logdir):
            rmtree(logdir)
        makedirs(logdir)

        # Create a dictionary to store the jobs for each data type
        jobs = {}

        # Create the jobs for each data type
        for type_name in self.register.data_containers.keys():
            # reg_content = self.register.acc_by_datatype[type_name]
            # print(f"Source {type_name} has {len(reg_content)} accessions to download.")
            # manipulator = self.datatype_manager.get_datacontainer(type_name)
            # if manipulator is None:
            #     self.logger.warning(f"No manipulator found for data type {type_name}. Skipping.")
            #     continue
            # Create jobs for each accession in the register
            container = self.register.data_containers[type_name]
            if len(container) > 0:
                jobs[type_name] = container.get_download_jobs(datadir)
                print(f"Created {len(jobs[type_name])} jobs for data type {type_name}.")

        # Create a JobManager instance
        manager = JobManager(max_process=max_process, log_folder=logdir, logger=self.logger)
        manager.start()

        # Add jobs to the JobManager in an interleaved way.
        # Doing this will allow the JobManager to start jobs from different sources in parallel.
        idxs = {source: 0 for source in jobs}
        total_jobs = sum([len(jobs[source]) for source in jobs])
        while total_jobs > 0:
            for source in jobs:
                if idxs[source] < len(jobs[source]):
                    manager.add_job(jobs[source][idxs[source]])
                    idxs[source] += 1
                    total_jobs -= 1

        # Wait for all jobs to complete
        while manager.remaining_jobs() > 0:
            time.sleep(1)

        # Stop and join the JobManager
        manager.stop()
        manager.join()

# -------------------- Utils downloads --------------------

def check_binary(path_to_bin: str) -> bool:
    """
    Check if the binary is present and executable

    :param: path_to_bin Path to the binary
    :return: True if the binary is present and executable. False otherwise.
    """
    try:
        cmd = f'{path_to_bin} --version'
        ret = subprocess.run(cmd.split(' '), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return ret.returncode == 0
    except FileNotFoundError:
        return False
