import subprocess
import time   

class TaskScheduler(object):
    EVENT_NAMES = ['before', 'started', 'done']

    def __init__(self):
        self.processes = {}
        self.stopped = False
        self.listeners = { event_name: [] for event_name in TaskScheduler.EVENT_NAMES }

    def _notify_before(self, name):
        for listener in self.listeners['before']:
            listener(name)

    def _notify_started(self, name):
        for listener in self.listeners['started']:
            listener(name)

    def _notify_done(self, name, errorcode):
        for listener in self.listeners['done']:
            listener(name, errorcode)

    def schedule(self, name, command, **kwargs):
        self._notify_before(name)
        self.processes[name] = subprocess.Popen(command, shell=True, bufsize=1, **kwargs)
        self._notify_started(name)

    def run(self):
        while not self.stopped:
            for name, process in self.processes.items():
                if process.poll() is not None:
                    self._notify_done(name, process.returncode)
                    del self.processes[name]
            time.sleep(1)

    def stop(self):
        self.stopped = True

    def running(self, job):
        return job in self.processes

    def on(self, event_name, event_handler):
        if event_name not in TaskScheduler.EVENT_NAMES:
            raise Exception('invalid event name: {0}'.format(event_name))
        self.listeners[event_name].append(event_handler)
