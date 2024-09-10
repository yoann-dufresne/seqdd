from os import listdir, makedirs, path
import re
from shutil import rmtree, move
import subprocess
from threading import Lock
import time

from seqdd.utils.scheduler import CmdLineJob, FunctionJob


naming = {
    'name': 'ENA',
    'key': 'ena',
    'classname': 'ENA'
}


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
        No binaries, always ready.
        """
        return True
    
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
    
    def wait_my_turn(self):
        """
        Waits for the minimum delay between ENA queries.
        WARNING: The function acquires the mutex lock. You must release it after using this function.
        """
        while not self.ena_delay_ready():
            time.sleep(0.01)
        self.mutex.acquire()

    
    # --- ENA Job creations ---

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

        # Each dataset download is independent
        for acc in accessions:
            job_name = f'ena_{acc}'
            # Create a temporary directory for the accession
            tmp_dir = path.join(self.tmpdir, acc)
            if path.exists(tmp_dir):
                rmtree(tmp_dir)
            makedirs(tmp_dir)

            # Get file urls to download
            urls = self.get_ena_ftp_url(acc)
            print(urls)
            
            # Creates a curl job for each URL
            curl_jobs = []
            for url in urls:
                # Get the file name from the URL
                filename = url.split('/')[-1]
                # Create the output file path
                output_file = path.join(tmp_dir, filename)
                # Create the command line job
                curl_jobs.append(CmdLineJob(
                    command_line=f'curl -o {output_file} "{url}"',
                    can_start = self.ena_delay_ready,
                    name=f'{job_name}_{filename}'
                ))
            jobs.extend(curl_jobs)
            
            # Create a function job to move the files to the final directory
            jobs.append(FunctionJob(
                func_to_run = self.move_and_clean,
                func_args = (tmp_dir, datadir),
                parents = curl_jobs,
                can_start = self.ena_delay_ready,
                name=f'{job_name}_move'
            ))

        return jobs
    
    
    def move_and_clean(self, accession_dir, outdir):
        """
        Moves the downloaded files from the accession directory to the output directory and cleans up the temporary directory.

        Args:
            accession_dir (str): The directory path containing the downloaded files.
            outdir (str): The output directory path.
            tmpdir (str): The temporary directory path.
        """
        # Enumerate all the files from the accession directory
        for filename in listdir(accession_dir):
            if filename.endswith('.gz'):
                move(path.join(accession_dir, filename), path.join(outdir, filename))

        # Clean the directory
        rmtree(accession_dir)
        
        
    def get_ena_ftp_url(self, accession):
        """
        Retourne l'URL FTP ENA à partir d'un numéro d'accession.
        """

        if accession.startswith("SRR") or accession.startswith("ERR") or accession.startswith("DRR"):
            # Wait for the minimal query delay
            self.wait_my_turn()
            # Query the ENA database to know the number of files to download (paired or single-end)
            query = f"https://www.ebi.ac.uk/ena/portal/api/filereport?accession={accession}&result=read_run&fields=run_accession,fastq_ftp&format=tsv"
            response = subprocess.run(['curl', query], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.mutex.release()
            # Fail on query error
            if response.returncode != 0:
                self.logger.error(f'Error querying ENA\nQuery: {query}\nAnswer: {response.stderr.decode()}\n{accession} will be skipped.')
                return []

            # Check if the sample is paired-end
            response = response.stdout.decode()
            lines = response.split('\n')
            header = lines[0].split('\t')
            fastq_ftp_index = header.index('fastq_ftp')
            
            # Search for the accession in the response
            for line in lines[1:]:
                if accession in line:
                    # Return the FTP URLs
                    return line.split('\t')[fastq_ftp_index].split(';')
                
            return []
            
        else:
            raise ValueError("Type d'accession non pris en charge.")
        

    # --- ENA accession validity ---
    
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
                if count == 0:
                    self.logger.warning(f'Invalid accession(s): {slice}')
                    continue
                # At least 2 accessions to verify
                if len(slice) > 1:
                    iter_size = len(slice)//2
                    valid_accessions.extend(self.accession_validity(slice[:iter_size], query_size=iter_size))
                    valid_accessions.extend(self.accession_validity(slice[iter_size:], query_size=iter_size))
            else:
                self.logger.info(f'Known valid accessions: {slice}')
                valid_accessions.extend(slice)

        return valid_accessions
    

    
