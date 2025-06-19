import argparse
import os
import platform
import re
import logging
import sys
from tempfile import gettempdir

from .register.reg_manager import save_accesions_to_source, create_register, Register
from .register.src_manager import SourceManager
from .utils.download import DownloadManager


def threads_available() -> int:
    """

    :return: The maximal number of threads available.
             It's nice with cluster scheduler or linux.
             On Mac it uses the number of physical cores
    """
    if hasattr(os, "sched_getaffinity"):
        threads_nb = len(os.sched_getaffinity(0))
    else:
        threads_nb = os.cpu_count()
    return threads_nb


def parse_cmd(logger: logging.Logger) -> argparse.Namespace:
    """
    Parse the command line

    :param logger: The object to log message
    :returns: the command line argument and options parsed
    """
    data_sources = [source.__name__ for source in SourceManager.get_sources()]
    parser = argparse.ArgumentParser(
                    prog='seqdd',
                    description='Prepare a sequence dataset, download it and export .reg files for reproducibility.',
                    epilog='Reproducibility is crucial, let\'s try to improve it!',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    subparsers = parser.add_subparsers(dest='cmd',
                                       required=True,
                                       help='command to apply')
    # Init register command
    init = subparsers.add_parser('init',
                                 help='Initialise the data register',
                                 formatter_class = argparse.ArgumentDefaultsHelpFormatter)
    init.add_argument('-f', '--force',
                      action='store_true',
                      help='Force reconstruction of the register')
    init.add_argument('-r', '--register-file',
                      type=str,
                      help='Init the local register from the register file')

    # Add entries to the register
    add = subparsers.add_parser('add',
                                help='Add dataset(s) to manage',
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add.add_argument('-s', '--source',
                     choices=data_sources,
                     help='Download source. Can download from ncbi genomes, '
                          'sra or an arbitrary url (uses wget to download)',
                     required=True)
    add.add_argument('-a', '--accessions',
                     nargs='+',
                     default=[],
                     help='List of accessions to register')
    add.add_argument('-f', '--file-of-accessions',
                     default="",
                     help='A file containing accessions to download, 1 per line')
    add.add_argument('-t', '--tmp-directory',
                     default=os.path.join(gettempdir(), 'seqdd'),
                     help='Temporary directory to store and organize the downloaded files')
    add.add_argument('--unitigs',
                     action='store_true',
                     help='Download unitigs instead of contigs for logan accessions.')

    # Download entries from the register
    download = subparsers.add_parser('download',
                                     help='Download data from the register. '
                                          'The download process needs sra-tools, ncbi command-line tools and wget.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    download.add_argument('-d', '--download-directory',
                          default='data', help='Directory where all the data will be downloaded')
    download.add_argument('-p', '--max-processes',
                          type=int,
                          default=threads_available() // 2,
                          help='Number of processes to run in parallel.')
    download.add_argument('-t', '--tmp-directory',
                          default=os.path.join(gettempdir(), 'seqdd'),
                          help='Temporary directory to store and organize the downloaded files')
    download.add_argument('--log-directory',
                          default='logs',
                          help='Directory where all the logs will be stored')

    # Export the register
    export = subparsers.add_parser('export',
                                   help='Export the metadata into a .reg file. '
                                        'This file can be loaded from other locations to download the exact same data.',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    export.add_argument('-o', '--output-register',
                        type=str, default='myregister.reg',
                        help='Name of the register file. Please prefer filenames .reg terminated.')

    # List the datasets from the register
    lst = subparsers.add_parser('list',
                                help='List all the datasets from the register. '
                                     'Subregisters are listed one after the other. '
                                     '5 accessions are displayed per line (tabulation separated).',
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    lst.add_argument('-s', '--source',
                     choices=data_sources,
                     help='List only the datasets from the given source. If not specified, list all the datasets.')
    lst.add_argument('-r', '--regular-expressions',
                     nargs='+',
                     default=[''],
                     help='List only the datasets accessions that match at least one of the given regular expressions')

    # Delete accessions from the register
    remove = subparsers.add_parser('remove',
                                   help='Remove dataset(s) from the register',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    remove.add_argument('-s', '--source',
                        choices=data_sources,
                        help='Delete only from the given source. If not specified, removed from all the sources.')
    remove.add_argument('-a', '--accessions',
                        nargs='+',
                        help='List of accessions to remove from the register. '
                             'Each accession can be a regular expression.')

    # Shared arguments
    for subparser in subparsers.choices.values():
        subparser.add_argument('--register-location',
                               default='.register',
                               help='Directory that store all info for the register')

    args = parser.parse_args()

    if args.cmd == 'add' and args.unitigs and args.source != 'logan':
        parser.error('--unitigs is only available for Logan source')

    return args


def on_remove(args: argparse.Namespace, logger: logging.Logger) -> None:
    """
    function corresponding to sub command `remove`

    :param args: The parsed cmd line arguments
    :param logger: The object to log
    """
     # validate the regexps
    valid_regexp = []
    for regexp in args.accessions:
        try:
            re.compile(regexp)
            valid_regexp.append(regexp)
        except re.error:
            logger.warning(f"Invalid regular expression {regexp}. Not used for search.")

    reg = Register(logger, dirpath=args.register_location)
    src_names = reg.acc_by_src.keys() if args.source is None else [args.source]
    for name in src_names:
        acc_lst = reg.filter_accessions(name, valid_regexp)
        for acc in acc_lst:
            reg.remove_accession(name, acc)
    reg.save_to_dir(args.register_location)


def on_list(args: argparse.Namespace, logger: logging.Logger) -> None:
    """
    function corresponding to 'list' sub command

    :param args: The parsed cmd line arguments
    :param logger: The object to log
    """
    # validate the regexps
    valid_regexp = []
    for regexp in args.regular_expressions:
        try:
            re.compile(regexp)
            valid_regexp.append(regexp)
        except re.error:
            logger.warning(f"Invalid regular expression {regexp}. Not used for search.")

    reg = Register(logger, dirpath=args.register_location)
    src_names = reg.acc_by_src.keys() if args.source is None else args.source
    for name in src_names:
        acc_lst = reg.filter_accessions(name, valid_regexp)

        if len(acc_lst) > 0:
            print(f"- {name}:")
            for idx in range(0, len(acc_lst), 5):
                current_slice = acc_lst[idx:idx+5]
                print("\t".join(current_slice))


def on_init(args: argparse.Namespace, logger:logging.Logger) -> None:
    """
    function corresponding to 'init' sub command

    :param args: The parsed cmd line arguments
    :param logger: The object to log
    """
    logger.info('Init register')
    location = args.register_location
    try:
        register = create_register(location, logger, force=args.force)
    except FileExistsError as err:
        sys.exit(str(err))

    if args.register_file is not None:
        register.load_from_file(args.register_file)
        register.save_to_dir(location)
    logger.info(f'Created at location {args.register_location}')


def on_add(args: argparse.Namespace, logger:logging.Logger) -> None:
    """
    function corresponding to 'add' sub command

    :param args: The parsed cmd line arguments
    :param logger: The object to log
    """
    # Getting the file to the sources
    src_path = os.path.join(args.register_location, f"{args.source}.txt")
    bin_dir = os.path.join(args.register_location, 'bin')
    # load the register
    src_mng = SourceManager(args.tmp_directory, bin_dir, logger)
    register = Register(logger, dirpath=args.register_location)

    # Load previous accession list
    accessions = register.acc_by_src.get(args.source, set())
    size_before = len(accessions)

    # Get the new accessions
    new_accessions = set()
    if len(args.accessions) > 0:
        new_accessions.update(args.accessions)
    if os.path.isfile(args.file_of_accessions):
        with open(args.file_of_accessions) as fr:
            new_accessions.update([x.strip() for x in fr if len(x.strip()) > 0])

    # Verification of the accessions
    src_manip = src_mng.sources[args.source]
    if args.unitigs:
        src_manip.set_option('unitigs', str(args.unitigs))
    valid_accessions = src_manip.filter_valid_accessions(frozenset(new_accessions))

    # Add valid accessions
    accessions.update(valid_accessions)
    logger.info(f"{len(accessions) - size_before} accessions added to the register")

    # Save the register
    if len(accessions) > size_before:
        save_accesions_to_source(src_path, accessions)


def on_download(args: argparse.Namespace, logger: logging.Logger) -> None:
    """
    function corresponding to 'download' sub command

    :param args: The parsed cmd line arguments
    :param logger: The object to log
    """
    max_threads_available = threads_available()
    if args.max_processes > max_threads_available:
        args.max_processes = max_threads_available
        logger.warning(f"The maximal number of threads available is {max_threads_available} "
                       f"set '--max-processes {max_threads_available}'.")
    bindir = os.path.join(args.register_location, 'bin')
    src_manager = SourceManager(args.tmp_directory, bindir, logger)
    reg = Register(logger, dirpath=args.register_location)
    dm = DownloadManager(reg, src_manager, logger, bindir, args.tmp_directory)
    dm.download_to(args.download_directory, args.log_directory , args.max_processes)


def on_export(args: argparse.Namespace, logger:logging.Logger) -> None:
    """
    function corresponding to 'export' sub command

    :param args: The parsed cmd line arguments
    :param logger: The object to log
    """
    reg = Register(logger, dirpath=args.register_location)
    reg.save_to_file(args.output_register)
    logger.info(f"Register exported to {args.output_register}")


def main() -> None:
    """
    main entry point to seqdd
    """
    # Platform check
    system = platform.system()
    if system == 'Windows':
        print('Windows plateforms are not supported by seqdd.', file=sys.stderr)
        exit(3)

    # Setup the logger
    logger = logging.getLogger('seqdd')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s - %(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    args = parse_cmd(logger)

    # Verify the existence of the data register
    if args.cmd != 'init':
        if not os.path.isdir(args.register_location):
            print('No data register found. Please first run the init command.', file=sys.stderr)
            exit(1)

    # Apply the right command
    cmd_to_apply = globals()[f"on_{args.cmd}"]
    cmd_to_apply(args, logger=logger)

if __name__ == "__main__":
    main()
