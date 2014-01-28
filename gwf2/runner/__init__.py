import os
import gwf2.conf as conf


class BaseStatus(object):
    def __init__(self):
        self.db = {}
        self.update()
    
    def update(self):
        """
        Update job db
        """
        raise Exception("Status.update must be implemented!")

    def lookup(self, jobid):
        if jobid in self.db:
            return self.db[jobid]
        else:
            return None

        

class BaseRunner(object):
    TEMPLATE_CMD                      = ''
    TEMPLATE_ARG_DEPENDENCY_LIST      = ''
    TEMPLATE_ARG_DEPENDENCY_SEPERATOR = ''
    TEMPLATE_ARG_JOBNAME              = ''
    TEMPLATE_ARG_QUEUE                = ''
    TEMPLATE_ARG_CORES                = ''
    TEMPLATE_ARG_WALLTIME             = ''
    TEMPLATE_ARG_CWD                  = ''
    TEMPLATE_SCRIPT_CONTENT           = ''

    def __init__(self, args):
        self.name       = args.name
        self.queue      = args.queue
        self.nodes      = args.nodes
        self.cores      = args.cores
        self.walltime   = args.walltime
        self.custom     = args.custom

    def dryrun(self, command, depIds):
        """
        Output command and job script. Return nothing.
        """
        raise Exception("Runner.dryrun must be implemented!")

    def submit(self, command, depIds):
        """
        Submit command to job system and return new job_id
        """
        raise Exception("Runner.submit must be implemented!")

    def create_command(self, cls, depIds):
        args = ''
        if depIds:
            args += cls.TEMPLATE_ARG_DEPENDENCY_LIST.format(depend=cls.TEMPLATE_ARG_DEPENDENCY_SEPERATOR.join(depIds))

        args += ' ' + cls.TEMPLATE_ARG_JOBNAME.format(name=self.name)
        args += ' ' + cls.TEMPLATE_ARG_QUEUE.format(queue=self.queue)
        args += ' ' + cls.TEMPLATE_ARG_CORES.format(nodes=self.nodes, cores=self.cores)
        args += ' ' + cls.TEMPLATE_ARG_WALLTIME.format(walltime=self.walltime)
        args += ' ' + cls.TEMPLATE_ARG_CWD.format(cwd=os.getcwd())

        jobcommand = cls.TEMPLATE_CMD.format(args=args, custom=self.custom)

        return jobcommand
    


config_scheduler = conf.get('scheduler')

if config_scheduler == 'PBS':
    from gwf2.runner.pbs import PBSRunner as Runner
    from gwf2.runner.pbs import PBSStatus as Status
else:
    raise Exception(config_scheduler)

