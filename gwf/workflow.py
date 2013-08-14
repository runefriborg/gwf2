'''Classes representing a workflow.'''

import sys
import os
import os.path
import time
import re
import string
import subprocess
import shutil
import logging
from copy import copy
from exceptions import NotImplementedError

from scheduler import TaskScheduler
from process import RemoteProcess
from process import local, remote
from dependency_graph import DependencyGraph

import parser  # need this to re-parse instantiated templates

logging.basicConfig(level=logging.DEBUG)


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

if 'PBS_JOB_ID' not in os.environ and \
    'PBS_NODEFILE' not in os.environ and \
        'GWF_SCRATCH' not in os.environ:
    logging.info('running in local mode')

    # fake a pbs job id
    os.environ['PBS_JOBID'] = str(time.clock())[2:12] + '.in'

    # fake a pbs node file
    import platform
    import multiprocessing
    cores = multiprocessing.cpu_count()

    with open('local_nodefile.tmp', 'w') as fp:
        for core in range(cores):
            print >> fp, platform.node()

    os.environ['PBS_NODEFILE'] = 'local_nodefile.tmp'

    # in local mode, we need something that corresponds to the scratch
    # directory, so we make one in the user's home directory, unless
    # something else is stated by the user.
    os.environ['GWF_SCRATCH'] = os.path.join(
        os.path.expanduser('~'), 'gwf-scratch')
    logging.info(
        'using fake scratch directory located in %s', os.environ['GWF_SCRATCH'])

PBS_JOB_ID = os.environ['PBS_JOBID']
PBS_NODEFILE = os.environ['PBS_NODEFILE']
GWF_SCRATCH = os.environ['GWF_SCRATCH']

if not os.path.exists(GWF_SCRATCH):
    os.mkdir(GWF_SCRATCH)

# TEMPLATES


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
            self.template[:80].replace('\n', ' ')
        )
    __repr__ = __str__  # not really the correct use of __repr__ but easy
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
            for key, val in self.assignments.items():
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
                for name, elms in expanded.items():
                    print name, '=', elms
                sys.exit(2)
            n = lengths.pop()

            new_assignments = dict()
            for key, val in self.assignments.items():
                if val in expanded:
                    new_assignments[key] = expanded[val]
                else:
                    new_assignments[key] = [val] * n

            targets = []
            for i in xrange(n):
                inst_assignments = \
                    dict((k, v[i]) for k, v in new_assignments.items())
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
    __repr__ = __str__  # not really the correct use of __repr__ but easy
                                   # for printing output when testing...

# VARIABLES LIST AND SUCH...


class List:

    def __init__(self, name, elements):
        self.name = name
        self.elements = elements

    def __str__(self):
        return '@list %s [%s]' % (
            self.name,
            ' '.join(self.elements)
        )
    __repr__ = __str__  # not really the correct use of __repr__ but easy
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
    __repr__ = __str__  # not really the correct use of __repr__ but easy
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
    __repr__ = __str__  # not really the correct use of __repr__ but easy
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
    __repr__ = __str__  # not really the correct use of __repr__ but easy
                                   # for printing output when testing...


# TASKS (TARGETS, FILES, AND ANYTHING THE WORKFLOW ACTUALLY SEES)
class Task(object):

    '''Abstract class for items in the workflow.'''

    def __init__(self, name, dependencies, wd):
        self.name = name
        self.dependencies = dependencies
        self.working_dir = wd
        self.is_dummy = False

        self._references = 0

    @property
    def references(self):
        return self._references

    @references.setter
    def references(self, value):
        self._references = value
        assert self._references >= 0, \
            "task cannot have negative number of references"

    @property
    def should_run(self):
        '''Test if this task needs to be run. Used when scheduling a
        dependency graph, but the specifics of when and how a task should
        run depends on the subclass'''
        raise NotImplementedError()

    @property
    def can_execute(self):
        '''Flag used to indicate that a task can be executed.

        Sub-classes needs to implement it if they are capable of execution.
        By default the answer is false, so if a task that cannot execute
        is scheduled, we can report an error.'''
        return False

    def get_input(self, task, in_file, dependency):
        raise NotImplementedError()

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
    def execution_error(self):
        return 'The file "%s" is not generated by any target, ' \
            'so it must be present on the file server.' % self.name

    def get_input(self, task, in_file, dependency):
        local_wd = task.local_wd

        relative_path = os.path.relpath(in_file, task.wd)
        base_dir = os.path.join(local_wd,
                                os.path.dirname(relative_path))

        if not os.path.exists(base_dir):
            logging.debug('creating directory structure %s',
                          os.path.join(local_wd,
                                       os.path.dirname(relative_path)))
            os.makedirs(base_dir)

        logging.debug("copying %s to %s",
                      in_file, os.path.join(local_wd, relative_path))
        shutil.copyfile(in_file, os.path.join(local_wd, relative_path))


class ExecutableTask(Task):

    '''Tasks that can be executed must provide this interface.'''

    @property
    def can_execute(self):
        return True


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
        self.host = None

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
        return True

        if not self.output:
            self.reason_to_run = \
                'Sinks (targets without output) should always run'
            return True  # If we don't provide output, assume we always
                        # need to run.

        for outf in self.output:
            if not _file_exists(_make_absolute_path(self.working_dir, outf)):
                self.reason_to_run = \
                    'Output file %s is missing' % outf
                return True

        for inf in self.input:
            if not _file_exists(_make_absolute_path(self.working_dir, inf)):
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
            timestamp = _get_file_timestamp(
                _make_absolute_path(self.working_dir, inf))
            if youngest_in_timestamp is None \
                    or youngest_in_timestamp < timestamp:
                youngest_in_filename = inf
                youngest_in_timestamp = timestamp
        assert youngest_in_timestamp is not None

        oldest_out_timestamp = None
        oldest_out_filename = None
        for outf in self.output:
            timestamp = _get_file_timestamp(
                _make_absolute_path(self.working_dir, outf))
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

    def get_input(self, task, in_file, dependency):
        # build path to file on remote source and destionation
        relpath = os.path.relpath(in_file, self.working_dir)
        src_path = os.path.join(dependency.local_wd, relpath)
        dst_path = os.path.join(task.local_wd, relpath)

        # figure out source and destination hosts
        src_host = dependency.host
        dst_host = task.host

        # if the dependency was checkpointed, we check if the file exists on
        # the node at which it was generated. If not, we should fetch the file
        # from remote storage.
        if dependency.checkpoint and not dependency.host:
            src_host = dst_host
            src_path = in_file

            if local('stat {0}'.format(src_path)) < 0:
                logging.error('''output of checkpointed
                              dependency does not exist at %s --
                              halting''' %
                              src_path)
                os.exit(1)

        # if the source host is the same as the destination host, we won't copy
        # any files, but just make a hardlink to the source file.
        if src_host == dst_host:
            logging.debug('making destination directory %s on host %s' %
                          (os.path.dirname(dst_path), dst_host))
            remote('mkdir -p {0}'.format(os.path.dirname(dst_path)),
                   dst_host)

            logging.debug('making hardlink from %s to %s on %s' %
                          (src_path, dst_path, src_host))
            remote('ln {0} {1}'.format(src_path, dst_path), src_host)
        else:
            # first create the destination directory on the destination node.
            logging.debug('making destination directory %s on host %s' %
                          (os.path.dirname(dst_path), dst_host))
            remote('mkdir -p {0}'.format(os.path.dirname(dst_path)),
                   dst_host)

            command = 'scp {0}:{1} {2}:{3}'.format(src_host,
                                                   src_path,
                                                   dst_host,
                                                   dst_path)
            logging.debug('%s' % command)
            local(command)

    @property
    def local_wd(self):
        return os.path.join(GWF_SCRATCH, PBS_JOB_ID, self.name)

    @property
    def cores(self):
        return 4

    @property
    def checkpoint(self):
        return 'checkpoint' in self.flags

    def __str__(self):
        return '@target %s, input(%s) -> output(%s)' % (
            self.name,
            ' '.join(self.input),
            ' '.join(self.output)
        )
    __repr__ = __str__  # not really the correct use of __repr__ but easy
                                   # for printing output when testing...


# WRAPPING IT ALL UP IN A WORKFLOW...
class Workflow(object):

    '''Class representing a workflow.'''

    def __init__(self,
                 lists,
                 templates,
                 targets,
                 template_targets,
                 wd,
                 target_name):
        self.lists = lists
        self.templates = templates
        self.targets = targets
        self.template_targets = template_targets
        self.working_dir = wd
        self.target_name = target_name

        # handle list transformation...
        for cmd in self.lists.values():
            if isinstance(cmd, Transform):
                input_list_name = cmd.input_list
                if input_list_name not in self.lists:
                    print "Transformation list %s uses input list %s the doesn't exist." %\
                        (cmd.name, input_list_name)
                    sys.exit(2)

                input_list = self.lists[input_list_name]

                cmd.elements = [re.sub(cmd.match_pattern, cmd.subs_pattern, input)
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
            target.input = expand_lists(target.input)
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
        with open(PBS_NODEFILE) as node_file:
            for node in node_file:
                node_name = node.strip()
                if not node_name in self.nodes:
                    self.nodes[node_name] = 0
                self.nodes[node_name] += 1

        # Compute the schedule and...
        target = self.targets[target_name]
        self.schedule, self.scheduled_tasks = \
            self.dependency_graph.schedule(target.name)

        # Build a list of all the jobs that have not been completed yet.
        # Jobs should be removed from this list when they have completed.
        self.missing = [job.task for job in self.schedule]

        # This list contains all the running jobs.
        self.running = []

        # ... then start the scheduler to actually run the jobs.
        self.scheduler = TaskScheduler()
        self.scheduler.on('before', self.on_before_job_started)
        self.scheduler.on('started', self.on_job_started)
        self.scheduler.on('done', self.on_job_done)

        # Now, schedule everything that can be scheduled...
        # NOTE: The copy is IMPORTANT since we modify missing
        #       during scheduling.
        self.schedule_tasks()
        self.scheduler.run()

    def schedule_tasks(self):
        '''Schedule all missing tasks.'''
        if not self.missing:
            self.scheduler.stop()
        for task in copy(self.missing):
            self.schedule_task(task)

    def schedule_task(self, task):
        '''Schedule a single task if all dependencies have been computed'''
        logging.debug('scheduling task=%s', task.name)

        # skip dummy tasks that we shouldn't submit...
        if task.dummy or not task.can_execute:
            return

        # if all dependencies are done, we may schedule this task.
        for _, dep_task in task.dependencies:
            if dep_task.can_execute:
                if dep_task in self.missing:
                    logging.debug(
                        'task not scheduled - dependency %s missing',
                        dep_task.name)
                    return
                if dep_task in self.running:
                    logging.debug(
                        'task not scheduled - dependency %s running',
                        dep_task.name)
                    return

        # schedule the task
        logging.debug("running task=%s cwd=%s code='%s'",
                      task.name, task.local_wd, task.code.strip())

        host = self.get_available_node(task.cores)

        # decrease the number of cores that the chosen node has available
        self.nodes[host] -= task.cores
        task.host = host

        process = RemoteProcess(task.code.strip(),
                                host,
                                stderr=subprocess.STDOUT,
                                cwd=task.local_wd)

        self.scheduler.schedule(task, process)

    def get_input(self, task):
        logging.info('fetching dependencies for %s' % task.name)
        for in_file, dependency in task.dependencies:
            dependency.get_input(task, in_file, dependency)

    def move_output(self, task):
        for out_file in task.output:
            relpath = os.path.relpath(out_file, self.working_dir)

            # figure out where this task was run and its path at that host
            src_host = task.host
            src_path = os.path.join(task.local_wd, relpath)

            # make the directory which we're going to copy to
            local('mkdir -p {0}'.format(os.path.dirname(out_file)))

            # now copy the file to the workflow working directory
            command = 'scp {0}:{1} {2}'.format(src_host,
                                               src_path,
                                               out_file)
            logging.debug('%s' % command)
            remote(command, src_host)

    def on_before_job_started(self, task):
        self.missing.remove(task)

        # move all input files to local working directory
        self.get_input(task)

    def on_job_done(self, task, errorcode):
        if errorcode > 0:
            logging.error(
                'task %s stopped with non-zero error code %s - halting',
                task.name, errorcode)
            self.scheduler.stop()

        # if this task is the final task, we should copy its output files to
        # the the workflow directory.
        if task.name == self.target_name or task.checkpoint:
            self.move_output(task)

        # decrease references for all dependencies of this task. Cleanup will
        # automatically be run for the dependency if its reference count is 0.
        for _, dependency in task.dependencies:
            if not isinstance(dependency, Target):
                continue
            dependency.references -= 1
            if dependency.references == 0:
                self.cleanup(dependency)

        # figure out where this task was run and increment the number of cores
        # available on the host, since the job is now done.
        host = task.host
        self.nodes[host] += task.cores

        self.running.remove(task)

        logging.info('task done: %s', task.name)

        # reschedule now that we know that a task has finished
        self.schedule_tasks()

    def cleanup(self, task):
        # figure out where the task was executed
        host = task.host

        # delete the task directory on the host
        logging.debug('deleting directory %s on host %s' %
                      (task.local_wd, host))
        remote('rm -rf {0}'.format(task.local_wd), host)

    def on_job_started(self, task):
        self.running.append(task)

    def get_available_node(self, cores_needed):
        for node, cores in self.nodes.iteritems():
            if cores >= cores_needed:
                return node
