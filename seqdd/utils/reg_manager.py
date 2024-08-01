from sys import stderr
from os import path, makedirs
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


def create_register(dirpath, force=False):
    # Remove files if force reconstruction
    if force and path.exists(dirpath):
        rmtree(dirpath)

    # Already existing register ?
    if path.exists(dirpath):
        print(f"A register is already present at location {dirpath}", file=stderr)
        exit(1)

    # Creates the directory if needed
    makedirs(dirpath, exist_ok=True)
    # Creates the subregisters
    reg = Register()
    # Save the subregisters
    reg.save_to_dir(dirpath)

    return reg


class Register:


    major_version = 0
    minor_version = 0

    def __init__(self, dirpath=None, regfile=None):
        self.subregisters = {
            'ncbi': set(),
            'sra': set(),
            'url': set()
        }

        if dirpath is not None:
            self.load_from_dir(dirpath)

        if regfile is not None:
            self.load_from_file(regfile)

    def load_from_dir(self, dirpath):
        if not path.isdir(dirpath):
            return False

        # Iterate over all the subregisters
        for key in self.subregisters:
            src_path = path.join(dirpath, f"{key}.txt")
            # Is the subregister exists ?
            if path.exists(src_path):
                # Load a subregister from its file
                self.subregisters[key].update(load_source(src_path))

        return True

    def save_to_dir(self, dirpath):
        if not path.isdir(dirpath):
            return False

        # Iterate over all the subregisters
        for key in self.subregisters:
            src_path = path.join(dirpath, f"{key}.txt")
            if len(self.subregisters[key]) > 0:
                # Save a subregister to its file
                save_source(src_path, self.subregisters[key])

        return True

    def save_to_file(self, file):
        with open(file, 'w') as fw:
            print(f'version {Register.major_version}.{Register.minor_version}', file=fw)
            # Iterate over all the subregisters
            for key in self.subregisters:
                if len(self.subregisters[key]) > 0:
                    print(f"{key}\t{len(self.subregisters[key])}", file=fw)
                    # Save the list of accessions
                    print('\n'.join(self.subregisters[key]), file=fw)

    def load_from_file(self, file):
        with open(file) as fr:
            # Version check
            prefix, version = fr.readline().strip().split(' ')
            if prefix != 'version':
                print('Missing version number at the beginning of the reg file. Skipping...', file=stderr)
                return
            major, minor = (int(x) for x in version.split('.'))
            if major != self.major_version:
                print(f'Incompatible versions. Your register is major version {major} while the tool awaits version {Register.major_version}. Skipping...', file=stderr)
                return
            if minor > Register.minor_version:
                print(f'Incompatible versions. Your register is major version {major}.{minor} while the tool awaits maximum version {Register.major_version}.{Register.minor_version} . Skipping...', file=stderr)
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

    def __repr__(self):
        return '\n'.join(f'{sub} : [{", ".join(self.subregisters[sub])}]' for sub in self.subregisters)
