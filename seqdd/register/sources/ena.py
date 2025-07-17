import logging
from os import listdir, makedirs, path
import re
from shutil import rmtree, move
import subprocess

from seqdd.register.sources import DataSource
from ...utils.scheduler import Job, CmdLineJob, FunctionJob
from ...errors import DownloadError



class ENA(DataSource):
    """
    The ENA class represents a data downloader for the European Nucleotide Archive (ENA) database.
    """

    # Regular expression for each type of ENA accession
    accession_patterns = {
        'Study': r'(E|D|S)RP[0-9]{6,}|PRJ(E|D|N)[A-Z][0-9]+',
        'Sample': r'(E|D|S)RS[0-9]{6,}|SAM(E|D|N)[A-Z]?[0-9]+',
        'Run': r'(E|D|S)RR[0-9]{6,}',
        'Experiment': r'(E|D|S)RX[0-9]{6,}',
        'Assembly': r'GCA_[0-9]{9}\.[0-9]+', # Assembly
        'Reference': r'GCF_[0-9]{9}\.[0-9]+', # Currated ref assembly
        'Submission': r'(E|D|S)RA[0-9]{6,}'
    }


    def __init__(self, tmpdir: str, bindir: str, logger: logging.Logger) -> None:
        """
        Initialize the ENA downloader object.

        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        :param bindir: The binary directory path. Where the helper binaries tools are stored.
        :param logger: The logger object.
        """
        super().__init__(tmpdir, bindir, logger, min_delay=0.35)


    # --- ENA Job creations ---

    def jobs_from_accessions(self, accessions: list[str], datadir: str) -> list[Job]:
        """
        Generates a list of jobs for downloading and processing ENA datasets.

        :param accessions: A list of ENA accessions.
        :param datadir: The output directory path. Where the expected files will be located.
        :returns: A list of jobs for downloading and processing ENA datasets.
        """
        jobs = []

        # Checking already downloaded accessions
        downloaded_accessions = frozenset(listdir(datadir))

        self.logger.info(f'Creating jobs for {len(accessions) - len(downloaded_accessions)} ENA accessions')

        # Each dataset download is independent
        for acc in accessions:
            # Skip already downloaded accessions
            if acc in downloaded_accessions:
                continue

            job_name = f'ena_{acc}'
            # Create a temporary directory for the accession
            tmp_dir = path.join(self.tmp_dir, acc)
            if path.exists(tmp_dir):
                rmtree(tmp_dir)
            makedirs(tmp_dir)

            # Check if the accession is an assembly and create jobs accordingly
            if acc.startswith('GCA') or acc.startswith('GCF'):
                jobs_from_assembly = self.jobs_from_assembly(acc, tmp_dir, datadir, job_name)
                jobs.extend(jobs_from_assembly)
                continue

            # Get file urls to download
            urls = self.get_ena_ftp_url(acc)

            # Creates a curl job for each URL
            curl_jobs = []
            md5s = dict()
            for url, md5 in urls:
                # Get the file name from the URL
                filename = url.split('/')[-1]
                # Store the MD5 hash
                md5s[filename] = md5
                # Create the output file path
                output_file = path.join(tmp_dir, filename)
                # Create the command line job
                curl_jobs.append(CmdLineJob(
                    command_line=f'curl -s -o {output_file} "{url}"',
                    can_start = self.source_delay_ready,
                    name=f'{job_name}_{filename}'
                ))
            jobs.extend(curl_jobs)

            # Create a function job to move the files to the final directory
            jobs.append(FunctionJob(
                func_to_run = self.move_and_clean,
                func_args = (tmp_dir, datadir, md5s),
                parents = curl_jobs,
                name=f'{job_name}_move'
            ))

        return jobs


    def jobs_from_assembly(self, assembly: str, tmpdir: str, outdir: str, job_name: str) \
            -> list[Job]:
        """
        Creates a list of jobs for downloading and processing an assembly.

        :param assembly: The assembly accession.
        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        :param outdir: The output directory path. Where the expected files will be located.
        :param job_name: The name of the job.
        :return: A list of jobs for downloading and processing an assembly.
        """
        # Get the assembly URL
        url = f'https://www.ebi.ac.uk/ena/browser/api/fasta/{assembly}'
        # Create the output file path
        output_file = path.join(tmpdir, f'{assembly}.fa')

        # Create the command line job
        curl_job = CmdLineJob(
            command_line=f'curl -o {output_file} "{url}"',
            can_start=self.source_delay_ready,
            name=f'{job_name}_{assembly}_download'
        )
        # Create a compression job
        gzip_job = CmdLineJob(
            command_line=f'gzip {output_file}',
            parents=[curl_job],
            name=f'{job_name}_{assembly}_gzip'
        )

        # Create a function job to move the files to the final directory
        move_job = FunctionJob(
            func_to_run=self.move_and_clean,
            func_args=(tmpdir, outdir),
            parents=[gzip_job],
            name=f'{job_name}_{assembly}_move'
        )

        return [curl_job, gzip_job, move_job]


    def move_and_clean(self, accession_dir: str, outdir: str, md5s: dict[str, str] | None = None) -> None:
        """
        Moves the downloaded files from the accession directory to the output directory and cleans
        up the temporary directory.

        :param accession_dir: The directory path containing the downloaded files.
        :param outdir: The output directory path. Where the expected files will be located.
        :param md5s: The md5s sum attached to each filename
        :type md5s: dict {filename str: md5 str}
        """
        if md5s is not None:
            # Validate the MD5 hashes
            for filename, md5 in md5s.items():
                file_path = path.join(accession_dir, filename)
                md5_check = subprocess.run(['md5sum', file_path], stdout=subprocess.PIPE)
                # Get the MD5 hash of the file
                md5_hash = md5_check.stdout.decode().split()[0]
                # Check if the MD5 hash is correct
                if md5 != md5_hash:
                    msg = (f'MD5 hash mismatch for file {filename} in accession {accession_dir}.\n'
                            'Accession files will not be downloaded.')
                    self.logger.error(msg)
                    rmtree(accession_dir)
                    raise DownloadError(msg)

        # Create the accession directory in the output directory
        outdir = path.join(outdir, path.basename(accession_dir))
        makedirs(outdir, exist_ok=True)

        filenames = listdir(accession_dir) if md5s is None else md5s.keys()
        # Enumerate all the files from the accession directory
        for filename in filenames:
            move(path.join(accession_dir, filename), path.join(outdir, filename))

        # Clean the directory
        rmtree(accession_dir)

    # --- ENA accession validity ---

    def filter_valid(self, accessions: list[str]) -> list[str]:
        """
        Filters the given list of ENA accessions and returns only the valid ones.

        :param accessions: A list of ENA accessions.
        :returns: A list of valid ENA accessions.
        """
        accessions_by_type = dict()
        for acc in accessions:
            acc_type = self.validate_accession(acc)
            if acc_type not in accessions_by_type:
                accessions_by_type[acc_type] = set()
            accessions_by_type[acc_type].add(acc)

        valid_accessions = []
        for acc_type in accessions_by_type:
            # Skip invalid accessions
            if acc_type == 'Invalid':
                continue

            # Server validation
            valid_accessions.extend(self.valid_accessions_on_API(list(accessions_by_type[acc_type])))

        return valid_accessions


    def valid_accessions_on_API(self, accessions: list[str], query_size: int = 32) -> list[str]:
        """

        :param accessions: The accessions to test
        :param query_size: The maximum number of accessions to validate at one time.
        :returns: list of accession that can be downloaded from ENA (after querying ENA for their presence)
        """
        valid_accessions = []
        query_begin = 'https://www.ebi.ac.uk/ena/browser/api/xml/'
        query_end = '?download=false&gzip=false&includeLinks=false'

        for i in range(0, len(accessions), query_size):
            slice = accessions[i:i+query_size]
            query = f'{query_begin}{",".join(slice)}{query_end}'
            # Wait for the delay
            self.wait_my_turn()
            # Query the ENA database
            response = None
            try:
                response = subprocess.run(['curl', query], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
            except subprocess.TimeoutExpired:
                self.end_my_turn()
                self.logger.error(f'Timeout querying ENA\nQuery: {query}')
                continue
            self.end_my_turn()

            if response.returncode != 0:
                self.logger.error(f'Error querying ENA\nQuery: {query}\nAnswer: {response.stderr.decode()}')
                continue

            # Parse the response
            response = response.stdout.decode()
            if 'ErrorDetails' in response:
                self.logger.error(f'Error querying ENA\nQuery: {query}\nAnswer: {response}')
                continue

            for acc in slice:
                if acc in response:
                    valid_accessions.append(acc)

        invalid_accessions = set(accessions) - set(valid_accessions)
        if len(invalid_accessions) > 0:
            self.logger.warning(f'Accession(s) not found on ENA servers: {", ".join(sorted(invalid_accessions))}')

        return valid_accessions


    def validate_accession(self, accession: str) -> str:
        """
        Validates a given accession.

        :param accession: The accession to validate.
        :returns: The type of accession if it is valid, otherwise the literal 'Invalid'.
        """
        for accession_type, pattern in ENA.accession_patterns.items():
            if re.fullmatch(pattern, accession):
                return accession_type
        self.logger.warning(f'Invalid accession: {accession}')
        return 'Invalid'


    # --- ENA FTP URL retrieval ---

    def get_ena_ftp_url(self, accession: str) -> list[tuple[str, str]]:
        """
        :param accession: The accession to download
        :returns: the ENA FTP URL(s) from an accession number.
        """
        # Query the ENA API to get the FTP URL(s) for fastq files
        query = f'https://www.ebi.ac.uk/ena/browser/api/xml/{accession}?download=false&gzip=false&includeLinks=false'
        self.wait_my_turn()
        response = subprocess.run(['curl', query], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.end_my_turn()

        # Check if the query was successful
        if response.returncode != 0:
            self.logger.error(f'Error querying ENA\nQuery: {query}\nAnswer: {response.stderr.decode()}')
            return []

        # Parse the response
        response = response.stdout.decode()
        # Get the url for fastq files
        match = re.search(r'<ID><!\[CDATA\[(https?://[^\]]+fastq_ftp[^\]]*)\]\]></ID>', response)
        if not match:
            self.logger.error(f'No fastq files found for accession {accession}')
            return []

        # Get the file list from the URL
        fastq_url = match.group(1)
        self.wait_my_turn()
        response = subprocess.run(['curl', fastq_url], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.end_my_turn()

        # Check if the query was successful
        if response.returncode != 0:
            self.logger.error(f'Error querying ENA\nQuery: {fastq_url}\nAnswer: {response.stderr.decode()}')
            return []

        # Parse the response
        lines = response.stdout.decode().strip().split('\n')
        if len(lines) < 2:
            return []

        # Get the header
        header = lines[0].split()
        if 'fastq_ftp' not in header or 'fastq_md5' not in header:
            self.logger.error(f'No fastq files found for accession {accession}')
            return []

        files = []
        # Get the FTP URLs and MD5 hashes
        for line in lines[1:]:
            data = line.split('\t')

            ftp_index = header.index('fastq_ftp')
            md5_index = header.index('fastq_md5')

            # If there is no download link, query the alias accession
            if len(data) <= ftp_index or len(data[ftp_index]) == 0:
                alias = data[header.index('run_accession')]
                self.logger.warning(f'No download link found for accession {alias} from the querry {accession}.')

            else:
                # Add the ftp URL and the md5 hash to the list
                ftp_urls = data[ftp_index].split(';')
                md5_hashes = data[md5_index].split(';')

                files.extend(zip(ftp_urls, md5_hashes))

        return files
