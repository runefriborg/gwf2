import os
import os.path
import platform
import logging
import json


class Environment(object):

    PROPERTIES = ['job_id', 'nodes', 'scratch_dir',
                  'config_dir', 'mother_node']

    @property
    def config_dir(self):
        return os.getenv('GWF_CONFIG_DIR',
                         os.path.expanduser('~/.gwf/'))

    @property
    def mother_node(self):
        return platform.node()

    def __repr__(self):
        return '{name}({values})'.format(name=__name__,
                                         values=str({p: self.__getattribute__(p)
                                                    for p in Environment.PROPERTIES}))

    def dump(self, path):
        logging.debug('dumping environment to {0}'.format(path))
        logging.debug('environment is {0}'.format(repr(self)))
        obj = {p: self.__getattribute__(p) for p in Environment.PROPERTIES}
        with open(path, 'w') as f:
            json.dump(obj, f)


class PBSEnvironment(Environment):

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

    @property
    def scratch_dir(self):
        # TODO, Move this to a global configuration setting
        return os.path.join(os.getenv('GWF_SCRATCH', '/scratch/'), self.job_id)


class FakeEnvironment(Environment):

    def __init__(self):
        '''Fakes the job id and node file.

        We cheat quite a lot here. The problem is that people may use
        the $PBS_JOBID variable directly in their target code, so we actually
        have to set it to maintain backwards compatability.'''

        # Lookup next jobid
        previous_job_id = 0
        if os.path.exists(os.path.join(os.path.expanduser('~'),".gwf/jobs")):
            previous_jobs = os.listdir(os.path.join(os.path.expanduser('~'),".gwf/jobs"))
            previous_jobs.sort()
            while(previous_jobs):
                try:
                    previous_job_id = int(previous_jobs.pop())
                    break
                except ValueError:
                    pass
                except IndexError:
                    pass
                previous_job_id = 0

        job_id_int = previous_job_id + 1
        self.job_id = "00000"[len(str(job_id_int)):]+str(job_id_int)

        os.environ['PBS_JOBID'] = self.job_id

        import multiprocessing
        import platform

        cores = multiprocessing.cpu_count()
        self.nodes = {platform.node(): cores}

    @property
    def scratch_dir(self):
        user_scratch_dir = os.path.join(os.getenv('GWF_SCRATCH',
                                        os.path.join(os.path.expanduser('~'),
                                                     ".gwf-scratch")),
                                        self.job_id)
        if not os.path.isdir(user_scratch_dir):
            os.makedirs(user_scratch_dir)
        return user_scratch_dir


def get_environment():
    # by default, we use a fake environment unless we figure out that there is
    # a real environment set by the queueing system.
    if os.getenv('PBS_JOBID', False) and os.getenv('PBS_NODEFILE', False):
        return PBSEnvironment()

    # Here we can add other environments for other batch systems, such as LoadLeveler.
    return FakeEnvironment()
