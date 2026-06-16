import logging
from contextlib import contextmanager, nullcontext
from os import path, makedirs
from shutil import rmtree
import time

from seqdd.utils.scheduler import JobManager
from seqdd.utils.manifest import write_manifest, MANIFEST_NAME
from seqdd.utils.progress import ProgressBar, human_bytes
from ..register.reg_manager import Register
from ..register.datatype_manager import DataTypeManager

# -------------------- Global download manager --------------------

class DownloadManager:
    """
    Class to handle download from different source
    """

    def __init__(self, register: Register, datatype_manager: DataTypeManager, logger: logging.Logger) -> None:
        """

        :param register:
        :param src_manager:
        :param logger: The logger object.
        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        """
        # The sources register
        self.register = register
        # The temporary directory path. Where the downloaded intermediate files are located.
        self.tmp_dir = datatype_manager.tmpdir
        # The datatype manager to handle different data types
        # This is used to get the source manager for each data type
        # and to handle the download of datasets from different sources.
        self.datatype_manager = datatype_manager
        self.logger = self.register.logger


    @contextmanager
    def _quiet_console_logs(self):
        """
        Temporarily raise the level of console (TTY) log handlers to WARNING.

        While the live progress bar owns the terminal, the per-job ``START``/``DONE`` INFO lines
        emitted by the JobManager (from another thread) would otherwise scramble it. Warnings and
        errors still get through. File handlers are untouched, and the original levels are restored
        on exit.
        """
        seqdd_logger = logging.getLogger('seqdd')
        raised = []
        for handler in seqdd_logger.handlers:
            stream = getattr(handler, 'stream', None)
            if stream is not None and hasattr(stream, 'isatty') and stream.isatty() \
                    and handler.level < logging.WARNING:
                raised.append((handler, handler.level))
                handler.setLevel(logging.WARNING)
        try:
            yield
        finally:
            for handler, level in raised:
                handler.setLevel(level)


    def download_to(self, datadir, logdir, max_process=8, dry_run=False) -> dict[str, int]:
        """
        Downloads datasets from different sources to the specified data directory.

        :param datadir: The path to the data directory where the datasets will be downloaded.
        :param logdir: The path to the log directory where the log files will be stored.
        :param max_process: The maximum number of processes to use for downloading. Defaults to 8.
        :param dry_run: If True, only report the planned work without downloading anything.
        :return: A dict with the number of 'completed', 'failed' and 'canceled' jobs.
        """
        # Dry run: report the planned work and stop before any side effect or network access
        if dry_run:
            self.logger.info('[dry-run] No data will be downloaded. Planned work:')
            total = 0
            n_types = 0
            for type_name, container in self.register.data_containers.items():
                if len(container) > 0:
                    self.logger.info(f'[dry-run]   {type_name}: {len(container)} accession(s)')
                    total += len(container)
                    n_types += 1
            self.logger.info(f'[dry-run] Total: {total} accession(s) across {n_types} data type(s).')
            return {'completed': 0, 'failed': 0, 'canceled': 0}

        # Creates the tmp and data directory if it doesn't exist
        makedirs(datadir, exist_ok=True)
        if logdir is not None and path.exists(logdir):
            rmtree(logdir)
        makedirs(logdir)

        # Create a dictionary to store the jobs for each data type
        jobs = {}

        # Create the jobs for each data type
        for type_name in self.register.data_containers.keys():
            container = self.register.data_containers[type_name]
            if len(container) > 0:
                jobs[type_name] = container.get_download_jobs(datadir)
                self.logger.info(f"Created {len(jobs[type_name])} job(s) for data type {type_name}.")

        # Create a JobManager instance
        manager = JobManager(max_process=max_process, log_folder=logdir, logger=self.logger)
        manager.start()

        # Add jobs to the JobManager in an interleaved way.
        # Doing this will allow the JobManager to start jobs from different sources in parallel.
        idxs = {source: 0 for source in jobs}
        n_jobs = sum(len(jobs[source]) for source in jobs)
        remaining_to_add = n_jobs
        while remaining_to_add > 0:
            for source in jobs:
                if idxs[source] < len(jobs[source]):
                    manager.add_job(jobs[source][idxs[source]])
                    idxs[source] += 1
                    remaining_to_add -= 1

        # Wait for all jobs to complete, reporting progress as the queue drains.
        # On an interactive terminal, draw a live one-line bar (finished/total jobs + bytes
        # downloaded) and silence the per-job INFO logs that would otherwise fight with it; on a
        # non-interactive stream (CI, pipes, log files) keep emitting a plain log line whenever the
        # count changes, with no carriage-return spam.
        progress = ProgressBar(n_jobs)
        downloaded = 0
        active = set()
        last_remaining = None
        with (self._quiet_console_logs() if progress.active else nullcontext()):
            while manager.remaining_jobs() > 0:
                for job_id, n_bytes in manager.poll_progress():
                    if n_bytes is None:
                        active.discard(job_id)
                    else:
                        downloaded += n_bytes
                        active.add(job_id)
                remaining = manager.remaining_jobs()
                done = n_jobs - remaining
                failed = len(manager.failed_jobs) + len(manager.canceled_jobs)
                if progress.active:
                    extra = f'  {human_bytes(downloaded)}'
                    if active:
                        extra += f'  ({len(active)} active)'
                    progress.update(done, failed, extra)
                elif remaining != last_remaining:
                    self.logger.info(f'Progress: {done}/{n_jobs} job(s) finished')
                    last_remaining = remaining
                time.sleep(0.25 if progress.active else 1)
            # Final drain for the last byte events, then settle the bar on its own line.
            for job_id, n_bytes in manager.poll_progress():
                if n_bytes is not None:
                    downloaded += n_bytes
            progress.close(n_jobs, len(manager.failed_jobs) + len(manager.canceled_jobs),
                           f'  {human_bytes(downloaded)}')

        # Stop and join the JobManager
        manager.stop()
        manager.join()

        # Final summary and outcome
        completed = len(manager.completed_jobs)
        failed = len(manager.failed_jobs)
        canceled = len(manager.canceled_jobs)
        if failed or canceled:
            self.logger.warning(
                f'Download finished with errors: {completed} succeeded, {failed} failed, '
                f'{canceled} canceled out of {n_jobs} job(s).'
            )
            broken = sorted(job.name for job in (manager.failed_jobs | manager.canceled_jobs))
            self.logger.warning('Failed or canceled job(s): ' + ', '.join(broken))
        else:
            self.logger.info(f'Download finished: {completed}/{n_jobs} job(s) succeeded.')

        # Write the provenance manifest (checksums) so the data set can be verified later
        self.logger.info(f'Writing provenance manifest ({MANIFEST_NAME})')
        write_manifest(datadir)

        return {'completed': completed, 'failed': failed, 'canceled': canceled}

# -------------------- Utils downloads --------------------

# def check_binary(path_to_bin: str) -> bool:
#     """
#     Check if the binary is present and executable

#     :param: path_to_bin Path to the binary
#     :return: True if the binary is present and executable. False otherwise.
#     """
#     try:
#         cmd = f'{path_to_bin} --version'
#         ret = subprocess.run(cmd.split(' '), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
#         return ret.returncode == 0
#     except FileNotFoundError:
#         return False
