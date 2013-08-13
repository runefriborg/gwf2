import subprocess
import time

from exceptions import NotImplementedError


class Process(object):
    def run(self):
        raise NotImplementedError()

    def kill(self):
        raise NotImplementedError()

    def poll(self):
        raise NotImplementedError()

    def wait(self):
        raise NotImplementedError()

    @property
    def running(self):
        raise NotImplementedError()

    @property
    def done(self):
        raise NotImplementedError()

    @property
    def returncode(self):
        raise NotImplementedError()


class LocalProcess(Process):
    def __init__(self, command, *args, **kwargs):
        self.command = command
        self.args = args
        self.kwargs = kwargs

    def run(self):
        self.process = subprocess.Popen(self.command,
                                        *self.args,
                                        shell=True,
                                        bufsize=1,
                                        **self.kwargs)

    def kill(self):
        self.process.kill()

    def poll(self):
        return self.process.poll()

    def wait(self):
        self.process.wait()

    @property
    def running(self):
        return self.poll() is None

    @property
    def done(self):
        return self.poll() is not None

    @property
    def returncode(self):
        return self.process.returncode


class RemoteProcess(LocalProcess):

    SSH_TEMPLATE = 'ssh {host} "cd {cwd} && {command}"'

    def __init__(self, command, host, *args, **kwargs):
        ssh_command = RemoteProcess.SSH_TEMPLATE.format(command=command,
                                                        host=host,
                                                        **kwargs)
        super(RemoteProcess, self).__init__(ssh_command, *args, **kwargs)


def remote(command, node, *args, **kwargs):
    process = RemoteProcess(command, node, *args, **kwargs)
    process.run()
    process.wait()


def local(command, *args, **kwargs):
    process = LocalProcess(command, *args, **kwargs)
    process.run()
    process.wait()


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
