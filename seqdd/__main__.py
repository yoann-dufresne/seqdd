import argparse
from os import path
from seqdd.utils.reg_manager import load_source, save_source, create_register


def parse_cmd():
    parser = argparse.ArgumentParser(
                    prog='seqdd',
                    description='Prepare a sequence dataset',
                    epilog='Allow better reproducibility')
    parser.add_argument('--register-location', default='.register', help='Directory that store all info for the register')
    subparsers = parser.add_subparsers(dest='cmd', required=True, help='command to apply')

    # Init register command
    init = subparsers.add_parser('init', help='Initialise the data register')
    init.add_argument('-f', '--force', action='store_true', help='Force reconstruction of the register')

    # Add entries to the register
    add = subparsers.add_parser('add', help='Add dataset(s) to manage')
    add.add_argument('-s', '--source', choices=['ncbi', 'sra', 'url'], help='Download source. Can download from ncbi genomes, sra or an arbitrary url (uses wget to download)', required=True)
    add.add_argument('-a', '--accessions', nargs='+', default=[], help='List of accessions to register')
    add.add_argument('-f', '--file-of-accessions', default="", help='A file containing accessions to download, 1 per line')

    # Download entries from the register
    download = subparsers.add_parser('download', help='Download data from the register. The download process needs sra-tools, ncbi command-line tools and wget.')

    args = parser.parse_args()
    return args


def on_init(args):
    print('Init register')
    location = args.register_location
    create_registry(location, force=args.force)


def on_add(args):
    src_path = path.join(args.register_location, f"{args.source}.txt")

    # Load previous accession list
    accessions = load_source(src_path)
    size_before = len(accessions)

    # Add the new accessions
    if len(args.accessions) > 0:
        accessions.update(args.accessions)
    if path.isfile(args.file_of_accessions):
        with open(args.file_of_accessions) as fr:
            accessions.update(x.strip() for x in fr if len(x.strip()) > 0)

    # Save the register
    if len(accessions) > size_before:
        save_source(src_path, accessions)


if __name__ == "__main__":
    args = parse_cmd()
    print(args)

    cmd_to_apply = locals()[f"on_{args.cmd}"]
    cmd_to_apply(args)