import os
import os.path
import time
import logging
import reporting

env = None
reporter = None


class Environment(object):

    @property
    def scratch_dir(self):
        return os.getenv('GWF_SCRATCH',
                         os.path.join(os.path.expanduser('~'), 'gwf-scratch'))

    @property
    def config_dir(self):
        return os.getenv('GWF_CONFIG_DIR',
                         os.path.expanduser('~/.gwf/'))

    def __repr__(self):
        properties = ['job_id', 'nodes', 'scratch_dir', 'config_dir']
        return '{name}({values})'.format(name=__name__,
                                         values=str({p: self.__getattribute__(p)
                                                    for p in properties}))


class RealEnvironment(Environment):

    def __init__(self):
        self.nodes = {}
        with open(os.environ['PBS_NODEFILE']) as node_file:
            for node in node_file:
                node_name = node.strip()
                if not node_name in self.nodes:
                    self.nodes[node_name] = 0
                self.nodes[node_name] += 1

    @property
    def job_id(self):
        return os.environ['PBS_JOBID']


class FakeEnvironment(Environment):

    def __init__(self):
        '''Fakes the job id and node file.

        We cheat quite a lot here. The problem is that people may use
        the $PBS_JOBID variable directly in their target code, so we actually
        have to set it to maintain backwards compatability.'''

        self.job_id = str(time.clock())[2:12] + '.in'
        os.environ['PBS_JOBID'] = self.job_id

        import multiprocessing
        import platform

        cores = multiprocessing.cpu_count()
        self.nodes = {platform.node(): cores}

if not env:
    if os.getenv('PBS_JOBID', False) and os.getenv('PBS_NODEFILE', False):
        env = RealEnvironment()
    else:
        env = FakeEnvironment()

    if not os.path.exists(env.scratch_dir):
        os.makedirs(env.scratch_dir)

    if not os.path.exists(env.config_dir):
        os.makedirs(env.config_dir)

    logging.debug('running using %s' % repr(env))

if not reporter:
    # Initialize file reporter such that it writes the log file to local
    # storage (scratch) until the reporter is finalized.
    tmp_dir = os.path.join(env.config_dir, 'jobs', env.job_id)
    final_dir = os.path.join(env.config_dir, 'jobs', env.job_id)
    reporter = reporting.FileReporter(tmp_dir=tmp_dir,
                                      final_dir=final_dir)

__all__ = ['env', 'reporter']
