import subprocess

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
    return process.returncode


def local(command, *args, **kwargs):
    process = LocalProcess(command, *args, **kwargs)
    process.run()
    process.wait()
    return process.returncode
