import time


class TaskScheduler(object):
    EVENT_NAMES = ['before', 'started', 'done']

    def __init__(self):
        self.processes = {}
        self.stopped = False
        self.listeners = {event_name: []
                          for event_name in TaskScheduler.EVENT_NAMES}

    def _notify_before(self, identifier):
        for listener in self.listeners['before']:
            listener(identifier)

    def _notify_started(self, identifier):
        for listener in self.listeners['started']:
            listener(identifier)

    def _notify_done(self, identifier, errorcode):
        for listener in self.listeners['done']:
            listener(identifier, errorcode)

    def schedule(self, identifier, process):
        self._notify_before(identifier)

        # register process and then run it.
        self.processes[identifier] = process
        process.run()

        self._notify_started(identifier)

    def run(self):
        while not self.stopped:
            for identifier, process in self.processes.items():
                if process.poll() is not None:
                    self._notify_done(identifier, process.returncode)
                    del self.processes[identifier]
            time.sleep(1)

    def stop(self):
        self.stopped = True

    def running(self, identifier):
        return identifier in self.processes

    def on(self, event_name, event_handler):
        if event_name not in TaskScheduler.EVENT_NAMES:
            raise Exception('invalid event identifier: {0}'.format(event_name))
        self.listeners[event_name].append(event_handler)
