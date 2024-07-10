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

    makedirs(dirpath, exist_ok=True)