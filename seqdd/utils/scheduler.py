from __future__ import annotations

from collections.abc import Iterable, Callable
import abc
import logging
from os import path
import sys
from threading import Thread
from multiprocessing import Process, Event
import subprocess
import time
from typing import Any


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
                    self.cancel_job(job)
                job.join()
                if job.get_returncode() == 0:
                    self.logger.info(f'DONE {job}')
                elif not notified:
                    self.logger.error(f'ERROR {job}\n{job.get_returncode()}')
                    self.logger.error(f'Please check the log file for more details: {job.log_file}')

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

    def cancel_job(self, job: Job) -> None:
        """

        :param job:
        """
        self.logger.warning(f'CANCEL {job}')
        # Cancel descendance
        if job in self.dependancies:
            for desc in self.dependancies[job]:
                self.cancel_job(desc)

        # cancel current job
        if job in self.running:
            self.running.remove(job)
        if job in self.waiting:
            self.waiting.remove(job)
        job.stop()        

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
        raise NotImplementedError()

    @abc.abstractmethod
    def stop(self) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_returncode(self)-> int | None:
        raise NotImplementedError()

    @abc.abstractmethod
    def join(self) -> None:
        raise NotImplementedError()


class FunctionJob(Job):
    """
    A Job class that wrap a function to run in a subprocess.
    """

    def __init__(self, func_to_run: Callable, func_args: tuple[Any, ...] = (),
                 parents: list[Job] = None, can_start: Callable = lambda:True,
                 name: str|None = None, log_file: str|None = None):
        """
        :param func_to_run: The function to run inside the subprocess.
        :param func_args: A tuple of arguments to give to the function to run
        :param parents: A list of parent jobs to wait before running this one.
        :param can_start: A function that is called when the job is ready and before starting it.
                         The function must return True when the job is allowed to start
        """
        name = name if name is not None else f'FunctionJob_{Job.ID}'
        log_file = log_file if log_file is not None else f'{name}.log'
        super().__init__(parents=parents, can_start=can_start, name=name, log_file=f'{name}.log')
        self.to_run = func_to_run
        self.args = func_args
        self.process = Process(target=self.wrapping_function, args=())


    def wrapping_function(self) -> None:
        """

        :return:
        """
        with open(self.log_file, 'w') as fw:
            sys.stdout = fw
            sys.stderr = fw
            print(f'Starting {self.to_run.__name__} with args {self.args}', file=fw)
            try:
                self.to_run(*self.args)
                print(f'Finished {self.to_run.__name__} with args {self.args}', file=fw)
            except Exception as e:
                print(f'Error in {self.to_run.__name__} with args {self.args}', file=fw)
                print(e, file=fw)
                raise e


    def start(self) -> None:
        """
        Start this job (in a subprocess)
        """
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
        