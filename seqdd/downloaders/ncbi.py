from os import listdir, makedirs, path
import platform
from shutil import rmtree, move
import subprocess
from sys import stderr
from threading import Lock
import time

from seqdd.utils.scheduler import CmdLineJob, FunctionJob
from seqdd.downloaders.download import check_binary
import json


class NCBI:
    """
    The NCBI class represents a data downloader for the National Center for Biotechnology Information (NCBI) database.

    Attributes:
        ncbi_joib_id (int): The ID counter for NCBI jobs.
        tmp_dir (str): The temporary directory path.
        bin_dir (str): The binary directory path.
        logger: The logger object for logging messages.
        mutex: The mutex lock for thread synchronization.
        bin (str): The path to the NCBI download software.
        last_ncbi_query (float): The timestamp of the last NCBI query.

    Methods:
        __init__(self, tmpdir, bindir, logger): Initializes a new instance of the NCBI class.
        is_ready(self): Checks if the NCBI download software is ready.
        ncbi_delay_ready(self): Checks if the minimum delay between NCBI queries has passed.
        jobs_from_accessions(self, accessions, dest_dir): Generates a list of jobs for downloading and processing accessions.
        clean(self, unzip_dir, dest_dir, tmp_dir): Cleans up the downloaded files and moves them to the destination directory.
        filter_valid_accessions(self, accessions): Filters and validates a list of accessions.
        get_download_software(self): Checks if the NCBI download software is installed and returns the path.
        install_datasets_software(self): Installs the NCBI download software if it is not already installed.
    """

    ncbi_joib_id = 0

    def __init__(self, tmpdir, bindir, logger):
        """
        Initializes a new instance of the NCBI downloader class.

        Args:
            tmpdir (str): The temporary directory path.
            bindir (str): The binary directory path.
            logger: The logger object for logging messages.
        """
        self.tmp_dir = tmpdir
        self.bin_dir = bindir
        self.logger = logger
        self.mutex = Lock()

        self.bin = self.get_download_software()
        self.last_ncbi_query = 0

    def is_ready(self):
        """
        Checks if the NCBI download software is ready.

        Returns:
            bool: True if the software is ready, False otherwise.
        """
        return self.bin is not None
    
    def ncbi_delay_ready(self):
        """
        Checks if the minimum delay between NCBI queries has passed.

        Returns:
            bool: True if the delay has passed, False otherwise.
        """
        # Minimal delay between ncbi queries (1s)
        min_delay = 1
        locked = self.mutex.acquire(blocking=False)
        ready = False
        if locked:
            # 5s since the last query ?
            ready = time.time() - self.last_ncbi_query > min_delay
            if ready:
                self.last_ncbi_query = time.time()
            self.mutex.release()
        return ready
    
    def jobs_from_accessions(self, accessions, dest_dir):
        """
        Generates a list of jobs for downloading and processing accessions.

        Args:
            accessions (list): The list of accessions to download.
            dest_dir (str): The destination directory path.

        Returns:
            list: A list of jobs for downloading and processing accessions.
        """
        accessions = list(accessions)
        all_jobs = []

        # Download accessions by batch of 5
        for idx in range(0, len(accessions), 5):
            # Create a temporary directory for the current job
            tmp_dir = path.join(self.tmp_dir, f'ncbi_{NCBI.ncbi_joib_id}')
            makedirs(tmp_dir, exist_ok=True)
            # Job name
            job_name = f'ncbi_job_{NCBI.ncbi_joib_id}'
            NCBI.ncbi_joib_id += 1

            # Take the right slice of 5 accessions
            acc_slice = accessions[idx:idx+5]

            # Download dehydrated job
            download_file = path.join(tmp_dir, f'{job_name}.zip')
            download_job = CmdLineJob(f"{self.bin} download genome accession --dehydrated --no-progressbar --filename {download_file} {' '.join(acc_slice)}", can_start=self.ncbi_delay_ready, name=f'{job_name}_download')
            
            # Unzip Job
            unzip_dir = path.join(tmp_dir, job_name)
            unzip_job = CmdLineJob(f"unzip -n {download_file} -d {unzip_dir}", parents=[download_job], name=f'{job_name}_unzip')

            # Data download
            rehydrate_job = CmdLineJob(f"{self.bin} rehydrate --gzip --no-progressbar --directory {unzip_dir}", parents=[unzip_job], can_start=self.ncbi_delay_ready, name=f'{job_name}_rehydrate')

            # Data reorganization
            reorg_job = FunctionJob(self.clean, func_args=(unzip_dir, dest_dir, tmp_dir), parents=[rehydrate_job], name=f'{job_name}_clean')

            all_jobs.extend([download_job, unzip_job, rehydrate_job, reorg_job])

        return all_jobs


    def clean(self, unzip_dir, dest_dir, tmp_dir):
        """
        Cleans up the downloaded files and moves them to the destination directory.

        Args:
            unzip_dir (str): The directory path where the files are unzipped.
            dest_dir (str): The destination directory path.
            tmp_dir (str): The temporary directory path.
        """
        # Remove subdirectories while moving their content
        data_dir = path.join(unzip_dir, "ncbi_dataset", "data")

        # Enumerated the downloaded files
        for subname in listdir(data_dir):
            subpath = path.join(data_dir, subname)
            # Looks only for datasets
            if path.isdir(subpath):
                # Looks only for datafiles
                for file in listdir(subpath):
                    if file.endswith(".gz"):
                        filepath = path.join(subpath, file)
                        # Move the data to its final destination
                        move(filepath, path.join(dest_dir, file))

        # Clean the download directory
        rmtree(tmp_dir)

    def filter_valid_accessions(self, accessions):
        """
        Filters and validates a list of accessions.

        Args:
            accessions (set): The set of accessions to filter and validate.

        Returns:
            set: The set of valid accessions.
        """
        accessions_list = list(accessions)
        valid_accessions = set()

        for idx in range(0, len(accessions), 10):
            # Accessions slice to validate
            accessions_slice = accessions_list[idx:idx+10]

            # Create a temporary directory for the current validation
            tmp_path = path.join(self.tmp_dir, f'ncbi_valid_{idx}')
            makedirs(tmp_path)
            archive_path = path.join(tmp_path, 'accessions.zip')

            # TODO: Wait the minimum delay between queries
            # Download the accessions info
            cmd = f'{self.bin} download genome accession {" ".join(accessions_slice)} --no-progressbar --include none --filename {archive_path}'
            ret = subprocess.run(cmd.split())

            # Check download status
            if ret.returncode != 0:
                self.logger.error(f'Datasets software error while downloading the accessions info: {ret.stderr}\nSkipping the validation of the accessions: {accessions_slice}')
                rmtree(tmp_path)
                continue

            # Unzip the accessions info
            unzip_path = path.join(tmp_path, 'accessions')
            cmd = f'unzip -qq {archive_path} -d {unzip_path}'
            ret = subprocess.run(cmd.split())

            # Check unzip status
            if ret.returncode != 0:
                self.logger.error(f'Impossible to unzip the accessions info: {archive_path}\nSkipping the validation of the accessions: {accessions_slice}')
                rmtree(tmp_path)
                continue

            # Check the accessions
            with open(path.join(unzip_path, 'ncbi_dataset', 'data', 'assembly_data_report.jsonl')) as fr:
                for line in fr:
                    # parse the json from the line
                    data = json.loads(line)
                    line_acc = data['accession']
                    if line_acc in accessions_slice:
                        valid_accessions.add(line_acc)

            # Clean the temporary directory
            rmtree(tmp_path)
            
        # Print the list of invalid accessions
        invalid_accessions = accessions - valid_accessions
        if len(invalid_accessions) > 0:
            self.logger.warning(f'The following accessions are skipped: {", ".join(list(invalid_accessions))}\nThose accessions will be ignored.')

        return valid_accessions
    

    def get_download_software(self):
        """
        Checks if the NCBI download software is installed and returns the path.

        Returns:
            str: The path to the NCBI download software, or None if it is not installed.
        """
        # Check if the system has the ncbi datasets cli
        system_installed = check_binary('datasets')
        if system_installed:
            return 'datasets'
        
        # Check if the software is locally installed
        local_bin = path.abspath(path.join(self.bin_dir, 'datasets'))
        locally_installed = check_binary(local_bin)
        if locally_installed:
            return f'{local_bin}'
        
        # Install the software
        return self.install_datasets_software()

    def install_datasets_software(self):
        """
        Installs the NCBI download software if it is not already installed.

        Returns:
            str: The path to the installed NCBI download software, or None if installation fails.
        """
        download_link = ''
        supported = True

        system = platform.system()
        if system == 'Linux':
            cpu_type = platform.machine()
            if cpu_type in ['i386', 'i686', 'x86_64', 'x86', 'AMD64']:
                download_link = 'https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-amd64/datasets'
            elif cpu_type in ['aarch64_be', 'aarch64', 'armv8b', 'armv8l', 'arm']:
                download_link = 'https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-arm64/datasets'
            else:
                supported = False
        else:
            supported = False

        # Message to potential system extensions
        if not supported:
            self.logger.error(f'ncbi datasets auto-install is not yet supported on your system. Plese install ncbi datasets cli by yourself. Also maybe we can include your system in the auto-installer. Please submit an issue on github with the following values:\nsystem={system}\tplateform={platform.machine()}')
            return None

        # Download datasets
        self.logger.info('Download the ncbi datasets cli binnary...')
        
        # Prepare the bin directory
        download_dir = path.abspath(self.bin_dir)
        makedirs(download_dir, exist_ok=True)
        
        # Download...
        cmd = f'curl -o {path.join(download_dir, "datasets")} {download_link}'
        ret = subprocess.run(cmd.split())

        if ret.returncode == 0:
            binpath = path.join(download_dir, 'datasets')

            ret = subprocess.run(f'chmod +x {binpath}'.split())
            if ret.returncode == 0:
                # move the binary to the bin directory
                final_path = path.abspath(path.join(self.bin_dir, 'datasets'))
                move(binpath, final_path)
                self.logger.info(f'ncbi datasets cli installed at {final_path}')

                return f'{final_path}'
            else:
                # Failed to set executable permissions
                self.logger.error(f'Failed to set executable permissions for ncbi datasets cli: {binpath}')
        else:
            # Failed to download ncbi datasets cli
            self.logger.error(f'Failed to download ncbi datasets cli from: {download_link}')

        return None
    