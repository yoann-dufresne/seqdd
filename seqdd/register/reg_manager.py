import logging
from os import path, makedirs, remove
from shutil import rmtree
from typing import Iterable
import re

from seqdd.register.datatype_manager import DataTypeManager



# --- Main register class ---

class Register:
    """
    A class representing a register.
    It is composed of subregisters from different data types (assemblies, read archives, diverse urls).
    """

    major_version = 0
    """The major version of the register."""
    minor_version = 1
    """minor_version (int): The minor version of the register."""


    def __init__(self, logger: logging.Logger, dirpath: str = None, regfile: str = None) -> None:
        """
        Initializes a Register object.

        :param logger: The logger object.
        :param dirpath: The directory path to load subregisters from.
        :param regfile: he file path to load the register from.
        """
        self.logger = logger

        # Initialize the subregisters
        self.data_type_manager = DataTypeManager(logger)
        self.data_containers = self.data_type_manager.get_data_types()
        
        # load data content
        if dirpath is not None:
            self.load_from_dir(dirpath)

        if regfile is not None:
            self.load_from_file(regfile)
        

    def __len__(self) -> int:
        """
        Returns the total number of accessions in all subregisters.

        :return: The total number of accessions in all subregisters.
        """
        return sum(len(dc) for dc in self.data_containers.values())
    

    def load_from_dir(self, dirpath: str) -> bool:
        """
        Loads subregisters from a directory.

        :param dirpath: The directory path to load subregisters from.
        :return: True if successful, False otherwise.
        """
        if not path.isdir(dirpath):
            self.logger.warning(f"Register {dirpath} does not exist.")
            return False

        # Iterate over all the subregisters
        for name, data_container in self.data_containers.items():
            src_path = path.join(dirpath, f"{name}.txt")
            # Is the subregister exists ?
            if path.exists(src_path):
                # Load a subregister from its file
                data_container.add_data(get_accessions_from_source(src_path))

        self.logger.debug(f'Register loaded from {dirpath}')
        return True


    def save_to_dir(self, dirpath: str) -> bool:
        """
        Saves subregisters to a directory.

        :param dirpath: The directory path to save subregisters to.
        :return: True if successful, False otherwise.
        """
        if not path.isdir(dirpath):
            self.logger.error(f"Register {dirpath} does not exist. Save aborted...")
            return False

        # Iterate over all the subregisters
        for name, data_container in self.data_containers.items():
            src_path = path.join(dirpath, f"{name}.txt")
            # If the subregister is not empty, save it
            if len(data_container) > 0:
                save_accesions_to_file(src_path, data_container.data)
            elif path.exists(src_path):
                # If the subregister is empty, remove the file
                remove(src_path)

        self.logger.debug(f'Register saved to {dirpath}')
        return True


    def save_to_file(self, file: str) -> None:
        """
        Saves the register to a file.

        :param file:
        :return: The file path to save the register to.
        """
        with open(file, 'w') as fw:
            print(f'version {Register.major_version}.{Register.minor_version}', file=fw)
            # Iterate over all the subregisters
            for name, data_container in self.data_containers.items():
                # If the subregister is not empty, save it
                if len(data_container) > 0:
                    print(f"{name}\t{len(data_container)}", file=fw)
                    print('\n'.join(data_container.data), file=fw)

        self.logger.debug(f'Datasets saved to register file {file}')


    def load_from_file(self, file: str) -> None:
        """
        Loads the register from a file.

        :param file: The file path to load the register from.
        """
        with open(file) as fr:
            # Version check
            prefix, version = fr.readline().strip().split()
            if prefix != 'version':
                self.logger.error('Missing version number at the beginning of the reg file. Skipping the loading')
                return
            major, minor = (int(x) for x in version.split('.'))
            if major != self.major_version:
                self.logger.error(f'Incompatible versions. '
                                  f'Your register is major version {major} while the tool awaits '
                                  f'version {Register.major_version}. Skipping the loading')
                return
            if minor > Register.minor_version:
                self.logger.error(f'Incompatible versions. Your register is major version {major}.{minor} while '
                                  f'the tool awaits maximum version {Register.major_version}.{Register.minor_version} .'
                                  f' Skipping the loading')
                return

            # Remaining line to read until the end of the current subregister
            remaining_to_read = 0
            current_register = None
            for line in fr:
                line = line.strip()
                # Skip empty lines
                if len(line) == 0:
                    continue
                # skip comment lines
                if line.startswith('#'):
                    continue
                # Waiting for the next register
                if remaining_to_read == 0:
                    split = line.split('\t')
                    if len(split) == 2:
                        current_register = split[0]
                        remaining_to_read = int(split[1])
                    else:
                        self.logger.error(f"Invalid line format in register file: {line}. Exiting loading.")
                        return
                # Add the next accession from the current register
                else:
                    if current_register not in self.data_containers:
                        self.logger.error(f"Unknown register {current_register} in the file {file}. Exiting loading.")
                        return
                    self.data_containers[current_register].add_data([line])
                    remaining_to_read -= 1

        self.logger.debug(f'Data from {file} successfully loaded')


    def remove_accession(self, source: str, accession: str) -> None:
        """
        Removes an accession from a source.

        :param source: The source to remove the accession from.
        :param accession: The accession to remove.
        """
        if source not in self.data_containers:
            self.logger.error(f"Data type {source} not found in the register.")
            return

        if accession in self.data_containers[source].data:
            self.data_containers[source].remove_data([accession])
            self.logger.info(f"Accession {accession} removed from {source}")
        else:
            self.logger.warning(f"Accession {accession} not found in {source}")


    def filter_accessions(self, source: str, regexps: list[str]) -> list[str]:
        """
        Returns the accessions from a given source that match at least one of the regexps.

        :param source: The source to filter.
        :param regexps: A list of regular expressions.
        :return:  A list of accessions from the source that match at least one of the regexps.
        """
        if source not in self.data_containers:
            self.logger.error(f"Source {source} not found in the register.")
            return []

        return [acc for acc in self.data_containers[source].data if any(re.match(regexp, acc) for regexp in regexps)]


    def __repr__(self) -> str:
        """

        :return: A string representation of the Register object.
        """
        return '\n'.join(f'{sub} : [{", ".join(self.data_containers[sub].data)}]' for sub in self.data_containers.keys())


# --- Register files load/save ---

def get_accessions_from_source(sourcepath: str) -> set[str]:
    """
    read the file corresponding to sourcepatth and get the accesions

    :param sourcepath: The path of this register corresponding a source
    :return: set of accession describe in the sourcepath
    """
    # Empty file
    if not path.isfile(sourcepath):
        return set()

    # Load accession set
    accessions = set()
    with open(sourcepath) as fr:
        for line in fr:
            acc = line.strip()
            if acc:
                accessions.add(acc)
    return accessions


def save_accesions_to_file(filepath: str, accessions: Iterable[str]) -> None:
    """
    write accessions to source corresponding to filepath. *Warning* if source path exist overwrite it.
    :param filepath: The path of this register corresponding a source
    :param accessions: The list of accessions to write in filepath
    """
    with open(filepath, 'w') as fw:
        for acc in accessions:
            print(acc, file=fw)


def create_register(dirpath: str, logger: logging.Logger, force: bool = False) -> Register:
    """

    :param dirpath: The path directory where to save The register
    :param logger: The logger object for logging messages.
    :param force: force register reconstruction
    :return:
    """
    # Remove files if force reconstruction
    if force and path.exists(dirpath):
        rmtree(dirpath)

    # Already existing register ?
    if path.exists(dirpath):
        msg = f"A register is already present at location {dirpath}"
        logger.critical(msg)
        raise FileExistsError(msg) from None

    # Creates the directory if needed
    makedirs(dirpath, exist_ok=True)
    # Creates the subregisters
    reg = Register(logger)
    # Save the subregisters
    reg.save_to_dir(dirpath)

    return reg
