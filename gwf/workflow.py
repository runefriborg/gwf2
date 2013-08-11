'''Classes representing a workflow.'''

import sys
import os, os.path
import time
import re
import string
import subprocess
import threading
import shutil
import logging as log
from exceptions import NotImplementedError

from dependency_graph import DependencyGraph
import parser # need this to re-parse instantiated templates

log.basicConfig(level=log.DEBUG)

def _escape_file_name(fname):
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    return ''.join(c for c in fname if c in valid_chars)
    
def _escape_job_name(jobname):
    valid_chars = "_%s%s" % (string.ascii_letters, string.digits)
    return ''.join(c for c in jobname if c in valid_chars)

def _file_exists(fname):
    return os.path.exists(fname)

def _get_file_timestamp(fname):
    return os.path.getmtime(fname)
    
def _make_absolute_path(working_dir, fname):
    if os.path.isabs(fname):
        abspath = fname
    else:
        abspath = os.path.join(working_dir, fname)
    return os.path.normpath(abspath)

## TEMPLATES 
class Template:
    def __init__(self, name, wd, parameters, template):
        self.name = name
        self.working_dir = wd
        self.parameters = parameters
        self.template = template
        
    def __str__(self):
        return '@template %s %s [%s...]' % (
            self.name,
            self.parameters,
            self.template[:80].replace('\n',' ')
            )
    __repr__ = __str__ # not really the correct use of __repr__ but easy 
    				   # for printing output when testing...
        
    
class TemplateTarget:
    def __init__(self, name, wd, template, parameter_assignments):
        self.name = name
        self.working_dir = wd
        self.template = template
        self.assignments = parameter_assignments
    
    def instantiate_target_code(self, workflow):
        '''Instantiate a target from the template. Uses "workflow" to access
        variable names like lists and templates.'''

        if self.template not in workflow.templates:
            print 'Template-target %s refers to unknown template %s' % \
                (self.name, self.template)
            sys.exit(2)        
        template_code = workflow.templates[self.template].template
        
        def instance(assignments):
            return 'target %s\n%s' % (_escape_job_name(self.name.format(**assignments)),
                                      template_code.format(**assignments))

        # If there are variables in the instantiation we must expand them 
        # here. If we are dealing with lists we get more than one target.

        has_variables = any(v.startswith('@')
                            for v in self.assignments.values())
                            
        if has_variables:
            # We have variables, so first we try to expand them.
            expanded = dict()
            for key,val in self.assignments.items():
                if val.startswith('@'):
                    listname = val[1:]
                    if listname not in workflow.lists:
                        print 'Template target %s refers unknown list %s' %\
                            (self.name, listname)
                        sys.exit(2)
                    elements = workflow.lists[listname].elements
                    expanded[val] = elements
            
            # Make sure the expanded lists have the same length...
            lengths = set(len(elms) for elms in expanded.values())
            if len(lengths) != 1:
                print 'The lists used in target template', self.name,
                print 'have different length.'
                for name,elms in expanded.items():
                    print name, '=', elms
                sys.exit(2)
            n = lengths.pop()
            
            new_assignments = dict()
            for key,val in self.assignments.items():
                if val in expanded:
                    new_assignments[key] = expanded[val]
                else:
                    new_assignments[key] = [val] * n
            
            targets = []
            for i in xrange(n):
                inst_assignments = \
                    dict((k,v[i]) for k,v in new_assignments.items())
                targets.append(instance(inst_assignments))
            return targets
            
        else:
            return [instance(self.assignments)]
    
    def __str__(self):
        return '@template-target %s %s %s' % (
            self.name,
            self.template,
            self.assignments
            )
    __repr__ = __str__ # not really the correct use of __repr__ but easy 
    				   # for printing output when testing...

## VARIABLES LIST AND SUCH...
class List:
    def __init__(self, name, elements):
        self.name = name
        self.elements = elements

    def __str__(self):
        return '@list %s [%s]' % (
            self.name,
            ' '.join(self.elements)
            )
    __repr__ = __str__ # not really the correct use of __repr__ but easy 
    				   # for printing output when testing...

class Glob(List):
    def __init__(self, name, glob_pattern, elements):
        List.__init__(self, name, elements)
        self.glob_pattern = glob_pattern

    def __str__(self):
        return '@glob %s %s [%s]' % (
            self.name,
            self.glob_pattern,
            ' '.join(self.elements)
            )
    __repr__ = __str__ # not really the correct use of __repr__ but easy 
    				   # for printing output when testing...

class Shell(List):
    def __init__(self, name, shell_command, elements):
        List.__init__(self, name, elements)
        self.shell_command = shell_command

    def __str__(self):
        return '@shell %s %s [%s]' % (
            self.name,
            self.shell_command,
            ' '.join(self.elements)
            )
    __repr__ = __str__ # not really the correct use of __repr__ but easy 
    				   # for printing output when testing...

class Transform(List):
    def __init__(self, name, match_pattern, subs_pattern, input_list, elements):
        List.__init__(self, name, elements)
        self.match_pattern = match_pattern
        self.subs_pattern = subs_pattern
        self.input_list = input_list

    def __str__(self):
        return '@transform %s %s %s %s [%s]' % (
            self.name,
            self.match_pattern, self.subs_pattern,
            self.input_list,
            ' '.join(self.elements)
            )
    __repr__ = __str__ # not really the correct use of __repr__ but easy 
    				   # for printing output when testing...


## TASKS (TARGETS, FILES, AND ANYTHING THE WORKFLOW ACTUALLY SEES)
class Task:
    '''Abstract class for items in the workflow.'''
    def __init__(self, name, dependencies, wd):
        self.name = name
        self.dependencies = dependencies
        self.working_dir = wd
        self.is_dummy = False

    @property
    def should_run(self):
        '''Test if this task needs to be run. Used when scheduling a
        dependency graph, but the specifics of when and how a task should
        run depends on the subclass'''
        raise NotImplementedError()

    @property
    def job_in_queue(self):
        '''Test if this task is a job already submitted to the queue'''
        raise NotImplementedError()

    @property
    def can_execute(self):
        '''Flag used to indicate that a task can be executed.
        
        Sub-classes needs to implement it if they are capable of execution.
        By default the answer is false, so if a task that cannot execute
        is scheduled, we can report an error.'''
        return False
        
    @property
    def dummy(self):
        return self.is_dummy
        
    @property
    def execution_error(self):
        '''Should return an error message if a task that cannot execute
        is scheduled for execution.'''
        return 'Unknown task type cannot be executed.'
    
    @property
    def done(self):
        '''Must return whether the task has been executed or not.'''
        return False

class SystemFile(Task):
    '''Class handling files that should be present on the system, i.e.
    files that are not generated by any targets but is specified as an
    input of one or more targets.'''
    
    def __init__(self, filename, wd):
        Task.__init__(self, filename, [], wd)
        self.is_dummy = True

    @property
    def file_exists(self):
        '''Check if the file exists. It is usually considered a major
        error if it doesn't since no target generates it.'''
        return _file_exists(_make_absolute_path(self.working_dir, self.name))

    @property
    def should_run(self):
        '''We should never actually run a system file, but we say yes when
        the file is missing so this is displayed in output.'''
        return not self.file_exists
        
    @property
    def job_in_queue(self):
        '''A file is never submitted to the queue...'''
        return False

    @property
    def execution_error(self):
        return 'The file "%s" is not generated by any target, ' \
            'so it must be present on the file server.' % self.name

    @property
    def done(self):
        return True


class ExecutableTask(Task):
    '''Tasks that can be executed must provide this interface.'''

    def can_execute(self):
        return True
    
    @property
    def script_name(self):
        '''Where is the script for executing the task located?'''
        raise NotImplementedError()

    @property
    def job_name(self):
        '''Where is job-id file located if the task is executing?'''
        raise NotImplementedError()
    
    def write_script(self):
        '''Write the script for executing the task to disk.'''
        raise NotImplementedError()

class Target(ExecutableTask):
    '''Class handling targets. Stores the info for executing them.'''
    
    def __init__(self, name, input, output, pbs_options, flags, code, wd):
        # passing None as dependencies, 'cause Workflow will fill it in
        Task.__init__(self, name, None, wd)
        self.input = input
        self.output = output
        self.pbs_options = pbs_options
        self.flags = flags
        self.code = code
        self.wd = wd

        self.done = False
        self.running = False
        
        if 'dummy' in self.flags and len(self.output) > 0:
            print 'Target %s is marked as a dummy target but has output files.'
            print 'Dummy targets will never be run so cannot produce output!'
            sys.exit(2)
        self.is_dummy = 'dummy' in self.flags

    @property
    def should_run(self):
        '''Test if this target needs to be run based on whether input
        and output files exist and on their time stamps. Doesn't check
        if upstream targets need to run, only this task; upstream tasks
        are handled by the dependency graph. '''
        
        
        if not self.output:
            self.reason_to_run = \
                'Sinks (targets without output) should always run'
            return True # If we don't provide output, assume we always
                        # need to run.

               
        for outf in self.output:
            if not _file_exists(_make_absolute_path(self.working_dir, outf)):
                self.reason_to_run = \
                    'Output file %s is missing' % outf
                return True

        for inf in self.input:
            if not _file_exists(_make_absolute_path(self.working_dir,inf)):
                self.reason_to_run = \
                    'Input file %s is missing' % outf
                return True

        # If no file is missing, it comes down to the time stamps. If we
        # only have output and no input, we assume the output is up to
        # date. Touching files and adding input can fix this behaviour
        # from the user side but if we have a program that just creates
        # files we don't want to run it whenever someone needs that
        # output just because we don't have time stamped input.

        if not self.input:
            self.reason_to_run = "We shouldn't run"
            return False

        # if we have both input and output files, check time stamps
        
        youngest_in_timestamp = None
        youngest_in_filename = None
        for inf in self.input:
            timestamp = _get_file_timestamp(_make_absolute_path(self.working_dir,inf))
            if youngest_in_timestamp is None \
                    or youngest_in_timestamp < timestamp:
                youngest_in_filename = inf
                youngest_in_timestamp = timestamp
        assert youngest_in_timestamp is not None

        oldest_out_timestamp = None
        oldest_out_filename = None
        for outf in self.output:
            timestamp = _get_file_timestamp(_make_absolute_path(self.working_dir,outf))
            if oldest_out_timestamp is None \
                    or oldest_out_timestamp > timestamp:
                oldest_out_filename = outf
                oldest_out_timestamp = timestamp
        assert oldest_out_timestamp is not None
        
        # The youngest in should be older than the oldest out
        if youngest_in_timestamp >= oldest_out_timestamp:
            # we have a younger in file than an outfile
            self.reason_to_run = 'Infile %s is younger than outfile %s' %\
                (youngest_in_filename, oldest_out_filename)
            return True
        else:
            self.reason_to_run = 'Youngest infile %s is older than '\
                                 'the oldest outfile %s' % \
                (youngest_in_filename, oldest_out_filename)
            return False    
            
        assert False, "We shouldn't get here"

    @property
    def job_name(self):
        # Escape name to make a file name...
        escaped_name = _escape_file_name(self.name)
        return _make_absolute_path(self.jobs_dir, escaped_name)

    def local_wd(self, pbs_job_id):
        return '/scratch/{0}/{1}'.format(pbs_job_id, self.name)

    @property
    def cores(self):
        return 16

    def __str__(self):
        return '@target %s, input(%s) -> output(%s)' % (
            self.name,
            ' '.join(self.input),
            ' '.join(self.output)
            )
    __repr__ = __str__ # not really the correct use of __repr__ but easy 
    				   # for printing output when testing...


## WRAPPING IT ALL UP IN A WORKFLOW...
class Workflow:
    '''Class representing a workflow.'''

    def __init__(self, lists, templates, targets, template_targets, wd):
        self.lists = lists
        self.templates = templates
        self.targets = targets
        self.template_targets = template_targets
        self.working_dir = wd

        self.pbs_job_id = os.environ['PBS_JOBID']

        self.pool = JobScheduler(started_handler=self.job_started,
                            stopped_handler=self.job_finished)
        self.pool.start()

        # handle list transformation...
        for cmd in self.lists.values():
            if isinstance(cmd, Transform):
                input_list_name = cmd.input_list
                if input_list_name not in self.lists:
                    print "Transformation list %s uses input list %s the doesn't exist."%\
                        (cmd.name, input_list_name)
                    sys.exit(2)
                
                input_list = self.lists[input_list_name]
                
                cmd.elements = [re.sub(cmd.match_pattern,cmd.subs_pattern,input)
                                for input in input_list.elements]

        # handle list expansions in other lists
        for cmd in self.lists.values():
            def expand_lists(lst):
                new_list = []
                for elm in lst:
                    if elm.startswith('@'):
                        listname = elm[1:]
                        if listname not in self.lists:
                            print 'List %s references unknown list %s.' % \
                                (cmd.name, listname)
                            sys.exit(2)
                        new_list.extend(self.lists[listname].elements)
                    else:
                        new_list.append(elm)
                return new_list
            cmd.elements = expand_lists(cmd.elements)


        # handle templates and template instantiations
        for name, tt in self.template_targets.items():
            for target_code in tt.instantiate_target_code(self):
                target = parser.parse_target(target_code, tt.working_dir)
                if target.name in self.targets:
                    print 'Instantiated template %s has the same name as an existing target' %\
                        target.name
                    sys.exit(2)
                self.targets[target.name] = target
                
        # expand lists in input and output lists for the targets
        for target in self.targets.values():
            def expand_lists(lst):
                new_list = []
                for elm in lst:
                    if elm.startswith('@'):
                        listname = elm[1:]
                        if listname not in self.lists:
                            print 'Target %s references unknown list %s.' % \
                                (target.name, listname)
                            sys.exit(2)
                        new_list.extend(self.lists[listname].elements)
                    else:
                        new_list.append(elm)
                return new_list
            target.input  = expand_lists(target.input)
            target.output = expand_lists(target.output)

            # make all files absolute and normalised so different ways of
            # referring to the same file actually works.
            # For obvious reasons this has to go after list expansion...
            target.input = [_make_absolute_path(target.working_dir, fname)
                            for fname in target.input]
            target.output = [_make_absolute_path(target.working_dir, fname)
                             for fname in target.output]


        # collect the output files so we know who can build them.
        self.providers = dict()
        for target in self.targets.values():
            for output_file in target.output:
                assert output_file not in self.providers
                self.providers[output_file] = target

        # now get dependencies for each target...
        for target in self.targets.values():
            dependencies = []
            for input_file in target.input:
                if input_file in self.providers:
                    dependencies.append((input_file,
                                         self.providers[input_file]))
                else:
                    sysfile = SystemFile(input_file, self.working_dir)
                    dependencies.append((input_file, sysfile))
            target.dependencies = dependencies
        
        # build the dependency graph    
        self.dependency_graph = DependencyGraph(self)

        # Figure out how many cores each allocated node has available. We need
        # this when scheduling jobs.
        self.nodes = {}
        with open(os.environ['PBS_NODEFILE']) as node_file:
            for node in node_file:
                node_name = node.strip()
                if not node_name in self.nodes:
                    self.nodes[node_name] = 0
                self.nodes[node_name] += 1

    def move_input_files(self, job):
        for in_file in job.task.input:
            local_wd = job.task.local_wd(self.pbs_job_id)
            relative_path = os.path.relpath(in_file, job.task.wd)
            try:
                os.makedirs(os.path.join(local_wd, os.path.dirname(relative_path)))
            except OSError as exc:
                pass
            log.debug("copying %s to %s", in_file, os.path.join(local_wd, relative_path))
            shutil.copyfile(in_file, os.path.join(local_wd, relative_path))

    def move_output_files(self, job):
        for out_file in job.task.output:
            relative_path = os.path.relpath(out_file, job.task.wd)
            absolute_path = os.path.join(job.task.local_wd(self.pbs_job_id), relative_path)
            log.debug('copying out file from %s to %s', absolute_path, out_file)
            shutil.copyfile(absolute_path, out_file)

    def job_finished(self, job):
        log.info('finished job: %s', job)
        self.move_output_files(job)
        job.task.done = True
        job.task.running = False

    def job_started(self, job):
        log.info('running job: %s', job)
        job.task.done = False
        job.task.running = True

    def run_workflow(self, target_name):
        '''Running the given workflow

        Running a workflow consists of scheduling the tasks such that
        all dependencies are fulfilled. To minimize IO to remote storage,
        we allocate machines dynamically and copy files directly between
        machines.
        '''
        target = self.targets[target_name]
        schedule, scheduled_tasks = self.dependency_graph.schedule(target.name)

        try:
            while not all(job.task.done for job in schedule):
                print {job.task.name: job.task.done for job in schedule}
                for job in schedule:
                    # skip dummy tasks that we shouldn't submit...
                    if job.task.done or job.task.running or job.task.dummy or not job.task.can_execute:
                        continue

                    # if all dependencies are done, we may schedule this job. This
                    # also handles the case of no dependencies since all([]) == True.
                    if not all(dep_job.done for resource, dep_job in job.task.dependencies if dep_job.can_execute):
                        continue

                    local_wd = job.task.local_wd(self.pbs_job_id)
                    try:
                        os.mkdir(local_wd)
                    except OSError as e:
                        log.warning("local working dir already exists at %s", local_wd)

                    # move all input files to local working directory
                    self.move_input_files(job)

                    # schedule the job
                    self.pool.schedule(job,
                                       job.task.code.strip(),
                                       stderr=subprocess.STDOUT,
                                       cwd=local_wd)

                log.debug('scheduling done, jobs left: %s', 
                          ', '.join(job.task.name for job in schedule if not job.task.done and not job.task.running))
                time.sleep(0.5)
        except:
            raise
        finally:
            self.pool.stop()

    def get_available_node(self, cores_needed):
        for node, cores in self.nodes.iteritems():
            if cores >= cores_needed:
                return node

class JobScheduler(threading.Thread):
    def __init__(self, started_handler, stopped_handler):
        threading.Thread.__init__(self)

        self.processes = {}
        self.stopped = False

        self.started_handler = started_handler
        self.stopped_handler = stopped_handler

    def schedule(self, name, command, **kwargs):
        self.processes[name] = subprocess.Popen(command, shell=True, **kwargs)
        self.started_handler(name)

    def run(self):
        while not self.stopped:
            for name, process in self.processes.items():
                if process.poll() is not None:
                    self.stopped_handler(name)
                    del self.processes[name]
            time.sleep(0.1)

    def stop(self):
        self.stopped = True

