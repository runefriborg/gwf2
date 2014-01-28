import sys
import subprocess
import re
from gwf2.runner import BaseRunner, BaseStatus

class PBSStatus(BaseStatus):    
    def update(self):
        self.db = {}

        process = subprocess.Popen("qstat", stdout=subprocess.PIPE)
        qstat_output, _ = process.communicate()
        
        _qstat_line_re = re.compile(r'^(?P<jobid>\d+)[^\d]+\s+'
                                '(?P<jobname>[^\s]+)\s+'
                                '(?P<username>[^\s]+)\s+'
                                '(?P<time_used>[^\s]+)\s+'
                                '(?P<state>[^\s]+)\s+'
                                '(?P<queue>[^\s]+)')
        for line in qstat_output.split('\n'):
            m = _qstat_line_re.match(line)
            if not m: continue
            
            # Got match
            self.db[m.group('jobid') + '.in'] = [m.group('jobname'), m.group('username'), m.group('time_used'), m.group('state'), m.group('queue')]



class PBSRunner(BaseRunner):

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
