import logging
import platform
import time
import json
import subprocess
import threading
from os import listdir, makedirs, path
from shutil import rmtree, move

from . import Source
from ...utils.scheduler import Job, CmdLineJob, FunctionJob
from ...utils.download import check_binary

# GCA_003774525.2 GCA_015190445.1 GCA_01519

naming = {
    'name': 'NCBI',
    'key': 'ncbi',
    'classname': 'NCBI'
}


class NCBI(Source):
    """
    The NCBI class represents a data downloader for the National Center for Biotechnology Information (NCBI) database.
    """

    ncbi_joib_id = 0


    def __init__(self, tmpdir: str, bindir: str, logger: logging.Logger) -> None:
        """
        Initializes a new instance of the NCBI downloader class.

        :param tmpdir: The temporary directory path.
        :param bindir: The binary directory path.
        :param logger: The logger object for logging messages.
        """
        super().__init__(tmpdir, bindir, logger)

        self.bin = self.get_download_software()
        """The path to the NCBI download software."""


    def is_ready(self) -> bool:
        """
        Checks if the NCBI download software is ready to be used.

        :return: True if the software is ready, False otherwise.
        """
        return self.bin is not None


    def src_delay_ready(self) -> bool:
        """
        Checks if the minimum delay between NCBI queries has passed.

        :return: True if the delay has passed, False otherwise.
        """
        # Minimal delay between ncbi queries (1s)
        min_delay = 1
        locked = self.mutex.acquire(blocking=False)
        ready = False
        if locked:
            # 5s since the last query ?
            ready = time.time() - self.last_query > min_delay
            if ready:
                self.last_query = time.time()
            self.mutex.release()
        return ready


    def wait_ncbi_delay(self) -> threading.Lock:
        """
        Wait for the NCBI ressource to be available (some delay between queries must be waited).
        Once the delay has passed, it acquires a mutex lock to ensure that no other operation is queriing instead.

        :return: The NCBI query lock.
        """
        while not self.src_delay_ready():
            time.sleep(time.time() - self.last_query)

        self.mutex.acquire()
        return self.mutex


    def jobs_from_accessions(self, accessions: list[str], dest_dir: str) -> list[Job]:
        """
        Generates a list of jobs for downloading and processing accessions.

        :param accessions: The list of accessions to download.
        :param dest_dir: The destination directory path.
        :return: A list of jobs for downloading and processing accessions.
        """
        to_download = []
        for acc in accessions:
            if path.exists(path.join(dest_dir, acc)):
                self.logger.warning(f'Accession {acc} already exists in the destination directory. Skipping download.')
            else:
                to_download.append(acc)
        all_jobs = []

        # Download accessions by batch of 5
        for idx in range(0, len(to_download), 5):
            # Create a temporary directory for the current job
            tmp_dir = path.join(self.tmp_dir, f'ncbi_{NCBI.ncbi_joib_id}')
            makedirs(tmp_dir, exist_ok=True)
            # Job name
            job_name = f'ncbi_job_{NCBI.ncbi_joib_id}'
            NCBI.ncbi_joib_id += 1

            # Take the right slice of 5 accessions
            acc_slice = to_download[idx:idx+5]

            # Download dehydrated job
            download_file = path.join(tmp_dir, f'{job_name}.zip')
            download_job = CmdLineJob(f"{self.bin} download genome accession --dehydrated --no-progressbar "
                                      f"--filename {download_file} {' '.join(acc_slice)}",
                                      can_start=self.src_delay_ready,
                                      name=f'{job_name}_download')

            # Unzip Job
            unzip_dir = path.join(tmp_dir, job_name)
            unzip_job = CmdLineJob(f"unzip -n {download_file} -d {unzip_dir}",
                                   parents=[download_job],
                                   name=f'{job_name}_unzip')

            # Data download
            rehydrate_job = CmdLineJob(f"{self.bin} rehydrate --gzip --no-progressbar --directory {unzip_dir}",
                                       parents=[unzip_job],
                                       can_start=self.src_delay_ready,
                                       name=f'{job_name}_rehydrate')

            # Data reorganization
            reorg_job = FunctionJob(self.clean,
                                    func_args=(unzip_dir, dest_dir, tmp_dir),
                                    parents=[rehydrate_job],
                                    name=f'{job_name}_clean')

            all_jobs.extend([download_job, unzip_job, rehydrate_job, reorg_job])

        return all_jobs


    def clean(self, unzip_dir: str, dest_dir: str, tmp_dir: str) -> None:
        """
        Cleans up the downloaded files and moves them to the destination directory.

        :param unzip_dir: The directory path where the files are unzipped.
        :param dest_dir: The destination directory path.
        :param tmp_dir: The temporary directory path.
        """
        # Remove subdirectories while moving their content
        data_dir = path.join(unzip_dir, "ncbi_dataset", "data")

        # Enumerated the downloaded files
        for subname in listdir(data_dir):
            subpath = path.join(data_dir, subname)
            # Looks only for datasets
            if path.isdir(subpath):
                # Move the directory and its content to the final directory
                move(subpath, path.join(dest_dir, subname))

        # Clean the download directory
        rmtree(tmp_dir)


    @staticmethod
    def is_valid_acc_format(acc: str) -> bool:
        r"""
        Check if the given accession number is in a valid format.
        An accession number is considered valid if it:
        - Starts with 'GCA\_' or 'GCF\_'
        - Contains a period ('.') separating the ID and version
        - The ID part is exactly 9 digits long
        - Both the ID and version parts are numeric

        :param acc: The accession number to validate.
        :return: True if the accession number is in a valid format, False otherwise.
        """
        if not (acc.startswith('GCA_') or acc.startswith('GCF_')):
            return False

        acc = acc[4:]
        if '.' not in acc:
            return False

        id, version = acc.split('.')
        if len(id) != 9 or not id.isdigit() or not version.isdigit():
            return False

        return True


    def filter_valid_accessions(self, accessions: set[str]) -> set[str]:
        """
        Filters and validates a list of accessions.

        :param accessions: The set of accessions to filter and validate.
        :return: The set of valid accessions.
        """
        accessions_list = [acc for acc in accessions if self.is_valid_acc_format(acc)]
        invalid_accessions = list(accessions - set(accessions_list))
        if len(invalid_accessions) > 0:
            self.logger.warning(f'Wrong format accessions: {", ".join(invalid_accessions)}. '
                                f'Expectiing GCA_XXXXXXXXX.Y or GCF_XXXXXXXXX.Y')

        valid_accessions = set()
        unknown_accessions = set()
        accessions_per_query = 32

        for idx in range(0, len(accessions), accessions_per_query):
            slice = accessions_list[idx:idx+accessions_per_query]

            # Query the NCBI to check if the accessions are valid
            cmd = f'{self.bin} summary genome accession {" ".join(slice)}'
            lock = self.wait_ncbi_delay()
            ret = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            lock.release()

            if ret.returncode != 0:
                self.logger.error(f'Failed to query NCBI for accessions: {", ".join(slice)}')
                continue

            # parse the json from the stout of the subprocess
            slice_set = set(slice)
            try:
                data = json.loads(ret.stdout)
                if 'reports' in data:
                    # Update accessions returned
                    for acc_obj in data['reports']:
                        acc = acc_obj['accession']
                        if acc in slice_set:
                            valid_accessions.add(acc)
                            slice_set.remove(acc)

                # Update unknown accessions
                unknown_accessions.update(slice_set)
            except json.JSONDecodeError:
                self.logger.error(f'Failed to parse the json response from NCBI for accessions: {", ".join(slice)}')

        if len(unknown_accessions) > 0:
            self.logger.warning(f'Unknown accessions: {", ".join(unknown_accessions)}')

        return valid_accessions


    def get_download_software(self) -> str|None:
        """
        Checks if the NCBI download software is installed and returns the path.

        :return: The path to the NCBI download software, or None if it is not installed.
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


    def install_datasets_software(self) -> str|None:
        """
        Installs the NCBI download software if it is not already installed.

        :return: The path to the installed NCBI download software, or None if installation fails.
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
            self.logger.error(f'ncbi datasets auto-install is not yet supported on your system. '
                              f'Plese install ncbi datasets cli by yourself. '
                              f'Also maybe we can include your system in the auto-installer. '
                              f'Please submit an issue on github with the following values:\nsystem={system}\tplateform={platform.machine()}')
            return None

        # Download datasets
        self.logger.info('Download the ncbi datasets cli binnary...')

        # Prepare the bin directory
        download_dir = path.abspath(self.bin_dir)
        makedirs(download_dir, exist_ok=True)

        # Download...
        cmd = f'curl -o {path.join(download_dir, "datasets")} {download_link}'
        ret = subprocess.run(cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

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
