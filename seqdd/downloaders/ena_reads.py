from os import listdir, makedirs, path, remove
import platform
import re
from shutil import rmtree, move
import subprocess
from sys import stderr
from threading import Lock
import time

from seqdd.downloaders.download import check_binary
from seqdd.utils.scheduler import CmdLineJob, FunctionJob



class ENA:
    """
    The ENA class represents a data downloader for the European Nucleotide Archive (ENA) database.

    Attributes:
        tmpdir (str): The temporary directory path.
        bindir (str): The binary directory path.
        logger: The logger object for logging messages.
        mutex: A lock object for thread synchronization.
        min_delay (float): The minimum delay between ENA queries in seconds.
        last_ena_query (float): The timestamp of the last ENA query.

    """
    
    def __init__(self, tmpdir, bindir, logger):
        """
        Initialize the ENA downloader object.

        Args:
            tmpdir (str): The temporary directory path.
            bindir (str): The binary directory path.
            logger: The logger object.
        """
        self.tmpdir = tmpdir
        self.bindir = bindir
        self.logger = logger
        
        self.mutex = Lock()
        self.min_delay = 0.35
        self.last_ena_query = 0

    def is_ready(self):
        """
        Checks if the SRA toolkit binaries are ready for use.

        Returns:
            bool: True if the binaries are ready, False otherwise.
        """
        return self.binaries is not None
    
    def ena_delay_ready(self):
        """
        Checks if the minimum delay between ENA queries has passed.

        Returns:
            bool: True if the minimum delay has passed, False otherwise.
        """
        # Minimal delay between SRA queries (0.5s)
        locked = self.mutex.acquire(blocking=False)
        ready = False
        if locked:
            ready = time.time() - self.last_ena_query > self.min_delay
            if ready:
                self.last_ena_query = time.time()
            self.mutex.release()
        return ready
    
    def get_ena_ftp_url(self, accession):
        """
        Retourne l'URL FTP ENA à partir d'un numéro d'accession.
        """

        if accession.startswith("SRR") or accession.startswith("ERR") or accession.startswith("DRR"):
            raise NotImplementedError

        else:
            raise ValueError("Type d'accession non pris en charge.")
    
    def filter_valid_accessions(self, accessions):
        """
        Filters the given list of ENA accessions and returns only the valid ones.

        Args:
            accessions (list): A list of ENA accessions.

        Returns:
            list: A list of valid ENA accessions.
        """

        valid_formats = frozenset(['SRR', 'ERR', 'DRR'])

        to_query = []

        for acc in accessions:
            # Check if the accession type is valid
            if not acc[:3] in valid_formats:
                self.logger.warning(f"{acc} is not a valid ENA reads accession.")
                continue
            to_query.append(acc)

        valid_accessions = self.accession_validity(to_query)
        print(valid_accessions)

        return valid_accessions

    def accession_validity(self, accessions, query_size=8):
        valid_accessions = []
        query_header = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=sra&term='

        for i in range(0, len(accessions), query_size):
            slice = accessions[i:i+query_size]
            query = f'{query_header}{"+OR+".join(slice)}'
            # Wait for the delay
            while not self.ena_delay_ready():
                time.sleep(0.01)
            self.mutex.acquire()
            # Query the ENA database
            response = subprocess.run(['curl', query], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if response.returncode != 0:
                self.logger.error(f'Error querying ENA\nQuery: {query}\nAnswer: {response.stderr.decode()}')
                continue
            # Update the last query time
            self.last_ena_query = time.time()
            self.mutex.release()
            # Parse the response
            response = response.stdout.decode()
            # Find the number of results inside of the response. It is present between the <Count> tags
            count = re.search(r'<Count>(\d+)</Count>', response)
            if count is None:
                self.logger.error(f'Error parsing ENA response\nQuery: {query}\nAnswer: {response}')
                continue
            count = int(count.group(1))
            # If at least 1 accession is wrong, search for the remaining valid ones by doing smaller queries.
            if count < len(slice):
                # At least 1 valid accession
                if count > 0 and len(slice) > 1:
                    iter_size = len(slice)//2
                    valid_accessions.extend(self.accession_validity(slice[:iter_size], query_size=iter_size))
                    valid_accessions.extend(self.accession_validity(slice[iter_size:], query_size=iter_size))
            else:
                self.logger.info(f'Known valid accessions: {slice}')
                valid_accessions.extend(slice)

        return valid_accessions
    
    def jobs_from_accessions(self, accessions, datadir):
        """
        Generates a list of jobs for downloading and processing ENA datasets.

        Args:
            accessions (list): A list of ENA accessions.
            datadir (str): The output directory path.

        Returns:
            list: A list of jobs for downloading and processing ENA datasets.
        """
        jobs = []

        raise NotImplementedError

        # Each dataset download is independent
        for acc in accessions:
            tmp_dir = path.join(self.tmpdir, acc)
            job_name = f'ena_{acc}'

            # Prefetch data
            cmd = f'{self.binaries["prefetch"]} --max-size u --output-directory {tmp_dir} {acc}'
            prefetch_job = CmdLineJob(cmd, can_start=self.sra_delay_ready, name=f'{job_name}_prefetch')

            # Move to datadir and clean tmpdir
            clean_job = FunctionJob(self.move_and_clean, func_args=(accession_dir, datadir, tmp_dir), parents=[compress_job], name=f'{job_name}_clean')

            # Set the jobs
            jobs.extend((prefetch_job, fasterqdump_job, compress_job, clean_job))

        return jobs

    
