import re
from sys import stderr
from os import path, makedirs, remove
from shutil import rmtree


def load_source(sourcepath):
    # Empty file
    if not path.isfile(sourcepath):
        return set()

    # Load accession set
    accessions = set()
    with open(sourcepath) as fr:
        for line in fr:
            acc = line.strip()
            if len(acc) > 0:
                accessions.add(acc)

    return accessions


def save_source(sourcepath, accessions):
    with open(sourcepath, 'w') as fw:
        for acc in accessions:
            print(acc, file=fw)


def create_register(dirpath, logger, force=False):
    # Remove files if force reconstruction
    if force and path.exists(dirpath):
        rmtree(dirpath)

    # Already existing register ?
    if path.exists(dirpath):
        logger.critical(f"A register is already present at location {dirpath}")
        exit(1)

    # Creates the directory if needed
    makedirs(dirpath, exist_ok=True)
    # Creates the subregisters
    reg = Register(logger)
    # Save the subregisters
    reg.save_to_dir(dirpath)

    return reg


class Register:
    """
    A class representing a register. It is composed of subregisters from different sources (NCBI genomes, SRA, diverse urls).

    Attributes:
        major_version (int): The major version of the register.
        minor_version (int): The minor version of the register.
        subregisters (dict): A dictionary containing the subregisters.

    Methods:
        __init__(dirpath=None, regfile=None): Initializes a Register object.
        load_from_dir(dirpath): Loads subregisters from a directory.
        save_to_dir(dirpath): Saves subregisters to a directory.
        save_to_file(file): Saves the register to a file.
        load_from_file(file): Loads the register from a file.
        __repr__(): Returns a string representation of the Register object.
    """

    major_version = 0
    minor_version = 0

    def __init__(self, logger, dirpath=None, regfile=None):
        """
        Initializes a Register object.

        Args:
            dirpath (str, optional): The directory path to load subregisters from. Defaults to None.
            regfile (str, optional): The file path to load the register from. Defaults to None.
        """
        self.subregisters = {
            'ncbi': set(),
            'sra': set(),
            'url': set()
        }

        self.logger = logger

        if dirpath is not None:
            self.load_from_dir(dirpath)

        if regfile is not None:
            self.load_from_file(regfile)

    def load_from_dir(self, dirpath):
        """
        Loads subregisters from a directory.

        Args:
            dirpath (str): The directory path to load subregisters from.

        Returns:
            bool: True if successful, False otherwise.
        """
        if not path.isdir(dirpath):
            self.logger.warning(f"Register {dirpath} does not exist.")
            return False

        # Iterate over all the subregisters
        for key in self.subregisters:
            src_path = path.join(dirpath, f"{key}.txt")
            # Is the subregister exists ?
            if path.exists(src_path):
                # Load a subregister from its file
                self.subregisters[key].update(load_source(src_path))

        self.logger.debug(f'Register loaded from {dirpath}')
        return True

    def save_to_dir(self, dirpath):
        """
        Saves subregisters to a directory.

        Args:
            dirpath (str): The directory path to save subregisters to.

        Returns:
            bool: True if successful, False otherwise.
        """
        if not path.isdir(dirpath):
            self.logger.error(f"Register {dirpath} does not exist. Save aborted...")
            return False

        # Iterate over all the subregisters
        for key in self.subregisters:
            src_path = path.join(dirpath, f"{key}.txt")
            if len(self.subregisters[key]) > 0:
                # Save a subregister to its file
                save_source(src_path, self.subregisters[key])
            elif path.exists(src_path):
                # Remove the file if the subregister is empty
                remove(src_path)

        self.logger.debug(f'Register saved to {dirpath}')
        return True

    def save_to_file(self, file):
        """
        Saves the register to a file.

        Args:
            file (str): The file path to save the register to.
        """
        with open(file, 'w') as fw:
            print(f'version {Register.major_version}.{Register.minor_version}', file=fw)
            # Iterate over all the subregisters
            for key in self.subregisters:
                if len(self.subregisters[key]) > 0:
                    print(f"{key}\t{len(self.subregisters[key])}", file=fw)
                    # Save the list of accessions
                    print('\n'.join(self.subregisters[key]), file=fw)

        self.logger.debug(f'Datasets saved to register file {file}')

    def load_from_file(self, file):
        """
        Loads the register from a file.

        Args:
            file (str): The file path to load the register from.
        """
        with open(file) as fr:
            # Version check
            prefix, version = fr.readline().strip().split(' ')
            if prefix != 'version':
                self.logger.error('Missing version number at the beginning of the reg file. Skipping the loading')
                return
            major, minor = (int(x) for x in version.split('.'))
            if major != self.major_version:
                self.logger.error(f'Incompatible versions. Your register is major version {major} while the tool awaits version {Register.major_version}. Skipping the loading')
                return
            if minor > Register.minor_version:
                self.logger.error(f'Incompatible versions. Your register is major version {major}.{minor} while the tool awaits maximum version {Register.major_version}.{Register.minor_version} . Skipping the loading')
                return
            
            # Remaining line to read until the end of the current subregister
            remaining_to_read = 0
            current_register = None
            for line in fr:
                line = line.strip()
                # New register
                if remaining_to_read == 0:
                    split = line.split('\t')
                    if len(split) == 2:
                        current_register = split[0]
                        remaining_to_read = int(split[1])
                # Add the next accession from the current register
                else :
                    self.subregisters[current_register].add(line)
                    remaining_to_read -= 1

        self.logger.debug(f'Data from {file} successfully loaded')

    
    def remove_accession(self, source, accession):
        """
        Removes an accession from a source.

        Args:
            source (str): The source to remove the accession from.
            accession (str): The accession to remove.
        """
        if source not in self.subregisters:
            self.logger.error(f"Source {source} not found in the register.")
            return

        if accession in self.subregisters[source]:
            self.subregisters[source].remove(accession)
            self.logger.info(f"Accession {accession} removed from {source}")
        else:
            self.logger.warning(f"Accession {accession} not found in {source}")


    def filter_accessions(self, source, regexps):
        """ Returns the accessions from a given source that match at least one of the regexps.

        Args:
            source (str): The source to filter.
            regexps (list): A list of regular expressions.

        Returns:
            list: A list of accessions from the source that match at least one of the regexps.
        """
        if source not in self.subregisters:
            self.logger.error(f"Source {source} not found in the register.")
            return []

        return [acc for acc in self.subregisters[source] if any(re.match(regexp, acc) for regexp in regexps)]


    def __repr__(self):
        """
        Returns a string representation of the Register object.

        Returns:
            str: A string representation of the Register object.
        """
        return '\n'.join(f'{sub} : [{", ".join(self.subregisters[sub])}]' for sub in self.subregisters)
