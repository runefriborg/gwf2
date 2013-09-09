import time

from events import Event


class ProcessScheduler(object):

    def __init__(self):
        self.processes = {}
        self._stopped = False

        self.before = Event()
        self.started = Event()
        self.done = Event()
        self.stopped = Event()

    def schedule(self, identifier, process):
        self.before(identifier)

        # register process and then run it.
        self.processes[identifier] = process
        process.run()

        self.started(identifier)

    def run(self):
        while not self._stopped:
            for identifier, process in self.processes.items():
                if process.poll() is not None:
                    self.done(identifier, process.returncode)
                    del self.processes[identifier]
            time.sleep(1)
        self.stopped()

    def stop(self):
        self._stopped = True

    def running(self, identifier):
        return identifier in self.processes
