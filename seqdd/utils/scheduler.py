from __future__ import annotations

from collections.abc import Iterable, Callable
import abc
import inspect
import logging
import multiprocessing as mp
import queue
from os import path
import sys
from threading import Thread
from multiprocessing import Event
import subprocess
import time
from typing import Any


def _process_context() -> mp.context.BaseContext:
    """
    Return a multiprocessing context that does not ``fork`` from this (multi-threaded) process.

    ``JobManager`` runs in a thread, so the default ``fork`` start method on Linux forks from a
    multi-threaded process: a child could deadlock on a lock held by another thread (e.g. the
    ``logging`` lock) at fork time, and Python 3.12+ warns about exactly this. ``forkserver``
    (preferred) forks clean children from a single-threaded server; ``spawn`` (Windows, or fallback)
    starts fresh interpreters. Both require picklable targets, which :class:`FunctionJob` already
    guarantees, so this is safe and removes both the warning and the latent deadlock.

    :return: A non-fork multiprocessing context.
    """
    available = mp.get_all_start_methods()
    for method in ('forkserver', 'spawn'):
        if method in available:
            return mp.get_context(method)
    return mp.get_context()


# Chosen once: the context used to run every FunctionJob subprocess.
_MP_CONTEXT = _process_context()


class JobManager(Thread):

    def __init__(self, logger: logging.Logger, max_process: int = 8, log_folder: str|None = None) -> None:
        """

        :param logger: The logger object.
        :param max_process: the maximum number of process to run at same time
        :param log_folder: The path to the directory where to write the logs
        """
        super().__init__()
        # Jobs queues
        self.processes = []
        self.waiting = []
        self.running = []
        self.dependancies = {}
        # Job outcome tracking (Job objects, identity-hashed)
        self.completed_jobs = set()
        self.failed_jobs = set()
        self.canceled_jobs = set()
        # Lazily-created multiprocessing queue collecting byte-progress events from jobs whose
        # target accepts a ``progress`` callback (created on the first such job in add_job).
        self.progress_queue = None
        # Logger
        self.max_process = max_process
        self.log_folder = path.abspath(log_folder) if log_folder is not None else None
        self.logger = logger
        # Boolean used to stop the thread
        self.stopped = Event()
        self.stopped.clear()


    def stop(self) -> None:
        """
        Stop this thread
        """
        self.stopped.set()


    def run(self) -> None:
        """
        Start a new thread
        """
        # Run the tasks
        while not self.stopped.is_set():
            # Check currently running processes
            to_remove = []
            for job in self.running:
                if not job.is_alive():
                    to_remove.append(job)

            # Remove finished jobs from running
            for job in to_remove:
                self.running.remove(job)
                notified = False
                if job.get_returncode() != 0:
                    self.logger.error(f'ERROR {job}\n{job.get_returncode()}')
                    self.logger.error(f'Please check the log file for more details: {job.log_file}')
                    notified = True
                    self.failed_jobs.add(job)
                    self.cancel_job(job)
                job.join()
                if job.get_returncode() == 0:
                    self.completed_jobs.add(job)
                    self.logger.info(f'DONE {job}')
                elif not notified:
                    self.logger.error(f'ERROR {job}\n{job.get_returncode()}')
                    self.logger.error(f'Please check the log file for more details: {job.log_file}')
                    self.failed_jobs.add(job)

            # Add new processes
            to_remove = []
            for job in self.waiting:
                # Max jobs reached
                if len(self.running) >= self.max_process:
                    break
                # Wait for all dependancies to be finished
                if not job.is_ready():
                    continue

                # Start a new job
                to_remove.append(job)
                self.running.append(job)
                self.logger.info(f'START {job}')
                job.start()

            # Remove jobs from waiting list
            for job in to_remove:
                self.waiting.remove(job)

            time.sleep(.1)

        # Clean the running jobs
        for job in self.running:
            if not job.is_over:
                job.stop()
                job.join()

        # Release the progress queue (if any) now that no job will report anymore.
        if self.progress_queue is not None:
            self.progress_queue.close()


    def cancel_job(self, job: Job) -> None:
        """
        Recursively cancel the whole descendance of a failed job. The failed job itself is left
        untouched (it already finished with an error and has been accounted as failed).

        :param job: The job whose descendants must be canceled.
        """
        for desc in self.dependancies.get(job, []):
            # Skip jobs already finished or already canceled (e.g. shared dependencies)
            if desc.is_over or desc in self.canceled_jobs:
                continue
            self.logger.warning(f'CANCEL {desc}')
            self.canceled_jobs.add(desc)
            if desc in self.running:
                self.running.remove(desc)
            if desc in self.waiting:
                self.waiting.remove(desc)
            desc.stop()
            # Recurse to cancel the descendants of this descendant
            self.cancel_job(desc)


    def add_job(self, process: Job):
        """
        add a new job and it's dependencies in the queue
        :param process: add a new job to run
        """
        # Modify the log file path
        if self.log_folder is not None:
            logfile_base = path.basename(process.log_file)
            logfile = path.join(self.log_folder, logfile_base)
            process.set_log_file(logfile)

        # Wire a progress channel for jobs whose target accepts a ``progress`` callback, so the
        # download functions can stream their byte counts back to the main process.
        if _job_reports_progress(process):
            if self.progress_queue is None:
                self.progress_queue = _MP_CONTEXT.Queue()
            process.progress_queue = self.progress_queue
            process.progress_id = process.name

        # Add the dependancies of the process
        for parent in process.parents:
            if parent not in self.dependancies:
                self.dependancies[parent] = []
            self.dependancies[parent].append(process)

        # Queue the process
        self.waiting.append(process)
        self.processes.append(process)


    def remaining_jobs(self) -> int:
        """
        :return: The number of job that are running or waiting to start
        """
        return len(self.waiting) + len(self.running)


    def poll_progress(self) -> list[tuple[str, int | None]]:
        """
        Drain the byte-progress queue without blocking.

        :return: A list of ``(job_id, n_bytes)`` events; ``n_bytes`` is None to mark a job's
                 download as finished. Empty when no job reports progress.
        """
        if self.progress_queue is None:
            return []
        events = []
        try:
            while True:
                events.append(self.progress_queue.get_nowait())
        except queue.Empty:
            pass
        return events


    def add_jobs(self, processes: Iterable[Job]) -> None:
        """
        Add several jobs in the queue
        :param processes: The jobs to add
        """
        for p in processes:
            # Add the process
            self.add_job(p)

    def __repr__(self):
        return f'running: {len(self.running)}\nwaiting: {len(self.waiting)}\ntotal: {len(self.processes)}\n{self.dependancies}'


class Job(metaclass=abc.ABCMeta):
    """
    A class to represent a Job.
    """

    ID = 0


    def __init__(self, name: str|None =None, parents: list[Job]|None =None,
                 can_start: Callable = lambda:True,
                 log_file:str|None = None) -> None:
        """

        :param name: The name of this job
        :param parents: the Jobs this job depends on, A list of parent jobs to wait before running this one.
        :param can_start: A function that is called when the job is ready and before starting it. The function must return True when the job is allowed to start
        :param log_file: the name of the file to write logs
        """
        self.name = name if name is not None else f'Job_{Job.ID}'
        self.log_file = log_file if log_file is not None else f'{self.name}.log'
        Job.ID += 1

        self.parents = [] if parents is None else parents
        """A list of parent jobs to wait before running this one."""
        self.is_over = False
        """True if the job is finished or canceled"""
        self.process = None
        """Subprocess that runs outside of the python program, depends on the job type"""
        self.can_start = can_start
        """A function that is called when the job is ready and before starting it. The function must return True when the job is allowed to start"""


    def set_log_file(self, log_file: str) -> None:
        self.log_file = log_file

    def is_ready(self) -> bool:
        """

        :return:  True if this Job is redy to start. False otherwise.
        """
        # Already over
        if self.is_over:
            return False
        # Parents are still running
        if not all(x.is_over for x in self.parents):
            return False
        # Are all the conditions to run present ?
        return self.can_start()

    @abc.abstractmethod
    def start(self) -> None:
        """Start this job (in a subprocess)"""
        raise NotImplementedError()

    @abc.abstractmethod
    def stop(self) -> None:
        """Wait until the job terminates."""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_returncode(self)-> int | None:
        """

        :return: This job returncode
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def join(self) -> None:
        """Wait until the job terminates."""
        raise NotImplementedError()


def _job_reports_progress(job: Job) -> bool:
    """
    :param job: A queued job.
    :return: True if the job runs a function that accepts a ``progress`` keyword (so it can report
             download progress over the JobManager's queue).
    """
    func = getattr(job, 'to_run', None)
    if func is None:
        return False
    try:
        return 'progress' in inspect.signature(func).parameters
    except (TypeError, ValueError):
        return False


def _queue_reporter(progress_queue: Any, job_id: str | None) -> Callable[[int], None]:
    """
    Build the per-chunk progress callback used inside a worker process.

    :param progress_queue: The queue receiving ``(job_id, n_bytes)`` events.
    :param job_id: The identifier tagging this worker's events.
    :return: A callback reporting the byte length passed to it.
    """
    def report(n_bytes: int) -> None:
        progress_queue.put((job_id, n_bytes))

    return report


def _run_function_job(func: Callable, args: tuple[Any, ...], log_file: str,
                      progress_queue: Any = None, job_id: str | None = None) -> None:
    """
    Module-level entry point executed inside the ``FunctionJob`` subprocess.

    It must stay at module level (and only receive picklable arguments) so the job is safe under
    the ``spawn`` multiprocessing start method used on Windows and recent macOS.

    When ``progress_queue`` is provided, a ``progress`` callback is built **inside the child** (so
    nothing unpicklable crosses the process boundary) and passed to ``func``; it reports the byte
    length of each downloaded chunk as ``(job_id, n_bytes)``, then a final ``(job_id, None)`` marker
    once the function returns or fails.

    :param func: The function to run.
    :param args: The positional arguments to pass to ``func``.
    :param log_file: The path of the file collecting the subprocess output.
    :param progress_queue: Optional multiprocessing queue receiving progress events.
    :param job_id: The identifier tagging this job's progress events.
    """
    with open(log_file, 'w') as fw:
        sys.stdout = fw
        sys.stderr = fw
        progress = None
        if progress_queue is not None:
            # Never let a slow or absent reader block this worker's exit on the queue feeder thread;
            # progress events are best-effort and may be dropped.
            progress_queue.cancel_join_thread()
            progress = _queue_reporter(progress_queue, job_id)
        print(f'Starting {func.__name__} with args {args}', file=fw)
        try:
            if progress is None:
                func(*args)
            else:
                func(*args, progress=progress)
            print(f'Finished {func.__name__} with args {args}', file=fw)
        except Exception as e:
            print(f'Error in {func.__name__} with args {args}', file=fw)
            print(e, file=fw)
            raise e
        finally:
            if progress_queue is not None:
                progress_queue.put((job_id, None))


class FunctionJob(Job):
    """
    A Job class that wrap a function to run in a subprocess.
    """

    def __init__(self, func_to_run: Callable, func_args: tuple[Any, ...] = (),
                 parents: list[Job] = None, can_start: Callable = lambda:True,
                 name: str|None = None, log_file: str|None = None):
        """
        :param func_to_run: The function to run inside the subprocess. It must be importable at
                            module level (picklable) for the ``spawn`` start method.
        :param func_args: A tuple of arguments to give to the function to run. Each argument must
                         be picklable (str, dict, …) for the ``spawn`` start method.
        :param parents: A list of parent jobs to wait before running this one.
        :param can_start: A function that is called when the job is ready and before starting it.
                         The function must return True when the job is allowed to start
        """
        name = name if name is not None else f'FunctionJob_{Job.ID}'
        log_file = log_file if log_file is not None else f'{name}.log'
        super().__init__(parents=parents, can_start=can_start, name=name, log_file=f'{name}.log')
        self.to_run = func_to_run
        self.args = func_args
        # Optional inter-process progress channel, wired by the JobManager for jobs whose target
        # accepts a ``progress`` callback. Left as None for jobs that do not report progress.
        self.progress_queue = None
        self.progress_id: str | None = None


    def start(self) -> None:
        """
        Start this job (in a subprocess).

        The process targets the module-level :func:`_run_function_job` with only picklable
        arguments, so nothing referencing this job (locks, the process itself, lambdas) is sent to
        the child. It runs under a non-fork context (:data:`_MP_CONTEXT`), which keeps the job
        working on every platform and avoids forking from the multi-threaded JobManager.
        """
        if self.progress_queue is None:
            proc_args = (self.to_run, self.args, self.log_file)
        else:
            proc_args = (self.to_run, self.args, self.log_file, self.progress_queue, self.progress_id)
        self.process = _MP_CONTEXT.Process(target=_run_function_job, args=proc_args)
        self.process.start()


    def stop(self) -> None:
        """
        Stop this job
        """
        self.is_over = True
        if self.process is None:
            return

        if self.process.is_alive():
            self.process.kill()
            self.process.join(timeout=5)
        if self.process.is_alive():
            self.process.terminate()


    def get_returncode(self) -> int | None:
        """

        :return: The job returncode
        """
        if self.process is None:
            return None
        return self.process.exitcode


    def join(self) -> None:
        """
        Wait until the job terminates.
        """
        self.process.join()


    def is_alive(self) -> bool:
        """

        :return: True if this Jb is running or waiting, False otherwise
        """
        alive = self.process.is_alive()
        if not alive:
            self.is_over = True
            self.process.join()
        return alive

    def __repr__(self):
        return f'FunctionJob [ {self.to_run.__name__} {self.args} ]'


class CmdLineJob(Job):
    """
    A Job class that wrap a command line to run in a subprocess.
    """


    def __init__(self, command_line: str, parents: list[Job]=None,
                 can_start: Callable = lambda:True,
                 name: str = None,
                 log_file: str = None) -> None:
        """

        :param command_line: A command line to run in a bash subprocess.
        :param parents: A list of parent jobs to wait before running this one.
        :param can_start: The function must return True when the job is allowed to start
        """
        name = name if name is not None else f'CmdLineJob_{Job.ID}'
        log_file = log_file if log_file is not None else f'{name}.log'
        super().__init__(parents=parents, can_start=can_start, name=name, log_file=log_file)
        self.cmd = command_line


    def start(self) -> None:
        """
        Start this job (in a subprocess).
        """
        with open(self.log_file, 'w') as fw:
            self.process = subprocess.Popen(self.cmd, shell=True, stdout=fw, stderr=fw)


    def stop(self) -> None:
        """
        Wait until the job terminates.
        """
        self.is_over = True
        if self.process is None:
            return

        self.process.kill()
        self.process.communicate(timeout=5)
        if self.process.returncode is None:
            self.process.terminate()


    def is_alive(self) -> bool:
        """

        :return: True if this Jb is running or waiting, False otherwise
        """
        alive = self.process.poll() is None
        if not alive:
            self.is_over = True
            self.process.communicate()
        return alive


    def get_returncode(self) -> int|None:
        """

        :return: The job returncode
        """
        if self.process is None:
            return None
        if self.is_alive():
            return None
        return self.process.returncode


    def join(self) -> None:
        """
        Wait until the job terminates.
        """
        self.process.communicate()


    def __repr__(self) -> str:
        return f'CmdLineJob [ {self.cmd} ]'
