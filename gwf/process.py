import sys
import subprocess
import logging

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

    @property
    def stderr(self):
        raise NotImplementedError()

    @property
    def stdout(self):
        raise NotImplementedError()


class LocalProcess(Process):
    def __init__(self, command, *args, **kwargs):
        self.command = command
        self.args = args
        self.kwargs = kwargs

    def run(self):
        logging.debug(self.command)
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

    def communicate(self, input=None):
        return self.process.communicate(input)

    @property
    def running(self):
        return self.poll() is None

    @property
    def done(self):
        return self.poll() is not None

    @property
    def returncode(self):
        return self.process.returncode

    @property
    def stderr(self):
        return self.process.stderr

    @property
    def stdout(self):
        return self.process.stdout


class RemoteProcess(LocalProcess):

    SSH_TEMPLATE = 'ssh -c arcfour {host} "cd {cwd} && {command}"'

    def __init__(self, command, host, *args, **kwargs):
        if 'cwd' not in kwargs:
            kwargs['cwd'] = '.'
        ssh_command = RemoteProcess.SSH_TEMPLATE.format(command=command,
                                                        host=host,
                                                        **kwargs)
        del kwargs['cwd']
        super(RemoteProcess, self).__init__(ssh_command, *args, **kwargs)


def remote(command, node, *args, **kwargs):
    process = RemoteProcess(command, node, *args, **kwargs)
    process.run()
    process.wait()
    return process.returncode

def remote2(command, node, stdindata, *args, **kwargs):
    kwargs['stdin'] = subprocess.PIPE
    process = RemoteProcess(command, node, *args, **kwargs)
    process.run()
    stdout, stderr = process.communicate(stdindata)
    if stdout:
        sys.stdout.write(stdout)
    if stderr:
        sys.stderr.write(stderr)
    return process.returncode

def local(command, *args, **kwargs):
    process = LocalProcess(command, *args, **kwargs)
    process.run()
    process.wait()
    return process.returncode
