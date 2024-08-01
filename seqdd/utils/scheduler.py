from sys import stderr
from threading import Thread
from multiprocessing import Process, Event
import subprocess
import time


class JobManager(Thread):

    def __init__(self, max_process=8):
        super().__init__()
        # Jobs queues
        self.processes = []
        self.waiting = []
        self.running = []
        self.dependancies = {}
        self.max_process = max_process
        # Boolean used to stop the thread
        self.stopped = Event()
        self.stopped.clear()

    def stop(self):
        self.stopped.set()

    def run(self):
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
                if job.get_returncode() != 0:
                    self.cancel_job(job)
                job.join()
                if job.get_returncode() == 0:
                    print('DONE', job)
                else:
                    print('ERROR', job, '\n', job.get_returncode(), file=stderr)

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
                print('START', job)
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

    def cancel_job(self, job):
        print('CANCEL', job)
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

    def add_process(self, process):
        # Add the dependancies of the process
        for parent in process.parents:
            if parent not in self.dependancies:
                self.dependancies[parent] = []
            self.dependancies[parent].append(process)

        # Queue the process
        self.waiting.append(process)
        self.processes.append(process)

    def remaining_jobs(self):
        return len(self.waiting) + len(self.running)

    def add_processes(self, processes):
        for p in processes:
            # Add the process
            self.add_process(p)

    def __repr__(self):
        return f'running: {len(self.running)}\nwaiting: {len(self.waiting)}\ntotal: {len(self.processes)}\n{self.dependancies}'


class Job:
    """
    A class to represent a Job.

    ...

    Attributes
    ----------
    process : Depends on the job type
        Subprocess that runs outside of the python program
    is_over : bool
        True if the job is finished or canceled
    parents : Array
        A list of parent jobs to wait before running this one.
    can_start : Function
        A function that is called when the job is ready and before starting it. The function must return True when the job is allowed to start
    """
    def __init__(self, parents=[], can_start=lambda:True):
        """
        Constructs all the necessary attributes for the person object.

        Parameters
        ----------
            parents : Array
                A list of parent jobs to wait before running this one.
            can_start : Function
                A function that is called when the job is ready and before starting it. The function must return True when the job is allowed to start
        """
        self.parents = parents
        self.is_over = False
        self.process = None
        self.can_start = can_start

    def is_ready(self):
        # Already over
        if self.is_over:
            return False
        # Parents are still running
        if not all(x.is_over for x in self.parents):
            return False
        # Are all the conditions to run present ?
        return self.can_start()

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def get_returncode(self):
        raise NotImplementedError()

    def join(self):
        raise NotImplementedError()


class FunctionJob(Job):
    '''
    A Job class that wrap a function to run in a subprocess.
    '''
    def __init__(self, func_to_run, func_args=(), parents=[], can_start=lambda:True):
        '''
            Parameters
            ----------
            func_to_run: Function
                The function to run inside the subprocess.
            func_args: Tuple
                A tuple of arguments to give to the function to run
            parents : Array
                A list of parent jobs to wait before running this one.
            can_start : Function
                A function that is called when the job is ready and before starting it. The function must return True when the job is allowed to start
        '''
        super().__init__(parents=parents, can_start=can_start)
        self.to_run = func_to_run
        self.args = func_args
        self.process = Process(target=func_to_run, args=func_args)

    def start(self):
        self.process.start()


    def stop(self):
        self.is_over = True
        if self.process is None:
            return

        if self.process.is_alive():
            self.process.kill()
            self.process.join(timeout=5)
        if self.process.is_alive():
            self.process.terminate()

    def get_returncode(self):
        if self.process is None:
            return None
        return self.process.exitcode

    def join(self):
        self.process.join()

    def is_alive(self):
        alive = self.process.is_alive()
        if not alive:
            self.is_over = True
            self.process.join()
        return alive

    def __repr__(self):
        return f'FunctionJob [ {self.to_run.__name__} {self.args} ]'


class CmdLineJob(Job):
    '''
    A Job class that wrap a command line to run in a subprocess.
    '''
    def __init__(self, command_line, parents=[], can_start=lambda:True):
        '''
            Parameters
            ----------
            command_line: string
                A command line to run in a bash subprocess.
            parents : Array
                A list of parent jobs to wait before running this one.
            can_start : Function
                A function that is called when the job is ready and before starting it. The function must return True when the job is allowed to start
        '''
        super().__init__(parents=parents, can_start=can_start)
        self.cmd = command_line

    def start(self):
        self.process = subprocess.Popen(self.cmd, shell=True)

    def stop(self):
        self.is_over = True
        if self.process is None:
            return

        self.process.kill()
        self.process.communicate(timeout=5)
        if self.process.returncode is None:
            self.process.terminate()

    def is_alive(self):
        alive = self.process.poll() is None
        if not alive:
            self.is_over = True
            self.process.communicate()
        return alive

    def get_returncode(self):
        if self.process is None:
            return None
        if self.is_alive():
            return None
        return self.process.returncode

    def join(self):
        self.process.communicate()

    def __repr__(self):
        return f'CmdLineJob [ {self.cmd} ]'
        