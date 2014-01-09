import sys
import subprocess
from gwf.runner import Runner

class PBSRunner(Runner):

    TEMPLATE_CMD                      = 'qsub {args} {custom}'
    TEMPLATE_ARG_DEPENDENCY_LIST      = '-W depend=afterok:{depend}'
    TEMPLATE_ARG_DEPENDENCY_SEPERATOR = ':'
    TEMPLATE_ARG_JOBNAME              = '-N {name}'
    TEMPLATE_ARG_QUEUE                = '-q {queue}'
    TEMPLATE_ARG_CORES                = '-l nodes={nodes}:ppn={cores}'
    TEMPLATE_ARG_WALLTIME             = '-l walltime={walltime}'
    TEMPLATE_ARG_CWD                  = '-d {cwd}'
    TEMPLATE_SCRIPT_CONTENT           = '#!/bin/sh\n{gwfcommand}'

    def dryrun(self, command, depIds):

        jobcommand = self.create_command(PBSRunner, depIds)
        complete_script = PBSRunner.TEMPLATE_SCRIPT_CONTENT.format(gwfcommand=command)

        sys.stdout.write(jobcommand + "\n")
        sys.stdout.write(complete_script + "\n")
        

    def submit(self, command, depIds):
                                                            
        jobcommand = self.create_command(PBSRunner, depIds)
        complete_script = PBSRunner.TEMPLATE_SCRIPT_CONTENT.format(gwfcommand=command)

        process = subprocess.Popen(jobcommand,
                                   shell=True,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)

        stdout, stderr = process.communicate(complete_script)

        if not stdout.strip().endswith('.in'):
            sys.stdout.write(stdout)
            sys.stderr.write(stderr)
            sys.exit(1)

        job_id = stdout.strip()

        return job_id
