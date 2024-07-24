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

            # Add new processes
            to_remove = []
            for job in self.waiting:
                # Max jobs reached
                if len(self.running) >= self.max_process:
                    break
                # Wait for all dependancies to be finished
                if not all(j.is_over for j in job.parents):
                    continue
                # Start a new job
                to_remove.append(job)
                self.running.append(job)
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
    def __init__(self, parents=[]):
        self.parents = parents
        self.is_over = False
        self.process = None

    def is_ready(self):
        return all(x.is_over in self.parents)

    def start(self):
        print(f'START {self}')

    def stop(self):
        print(f'STOP {self}')

    def get_returncode(self):
        raise NotImplementedError()

    def join(self):
        raise NotImplementedError()


class FunctionJob(Job):
    def __init__(self, func_to_run, func_args=(), parents=[]):
        super().__init__(parents=parents)
        self.to_run = func_to_run
        self.args = func_args
        self.process = Process(target=func_to_run, args=func_args)

    def start(self):
        super().start()
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
        super().stop()

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

    def __init__(self, command_line, parents=[]):
        super().__init__(parents=parents)
        self.cmd = command_line

    def start(self):
        super().start()
        self.process = subprocess.Popen(self.cmd.split(' '))

    def stop(self):
        self.is_over = True
        if self.process is None:
            return

        self.process.kill()
        self.process.communicate(timeout=5)
        if self.process.returncode is None:
            self.process.terminate()
        super().stop()

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
        