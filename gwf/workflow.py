'''Classes representing a workflow.'''

import sys
import os
import os.path
import re
import string
import shutil
import logging
import platform
from exceptions import NotImplementedError

import parser  # need this to re-parse instantiated templates

from process import remote, local
from events import Event


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


# TEMPLATES


class Template(object):

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


class TemplateTarget(object):

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
            return 'target %s\n%s' % (_escape_job_name(
                                      self.name.format(**assignments)),
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


class List(object):

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
        self.host = platform.node()

        self.transfer_started = Event()
        self.transfer_failed = Event()
        self.transfer_success = Event()

        self._references = 0

    @property
    def checkpoint(self):
        return False

    @property
    def references(self):
        return self._references

    @references.setter
    def references(self, value):
        self._references = value
        assert self._references >= 0, \
            "task cannot have negative number of references"

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
    def youngest_time(self):
        return _get_file_timestamp(_make_absolute_path(self.working_dir, self.name))

    oldest_time = youngest_time

    @property
    def execution_error(self):
        return 'The file "%s" is not generated by any target, ' \
            'so it must be present on the file server.' % self.name

    @property
    def local_wd(self):
        return self.working_dir

    def get_input(self):
        for source, dependency in self.dependencies:
            try:
                local_wd = self.local_wd

                relative_path = os.path.relpath(source, self.wd)
                base_dir = os.path.join(local_wd,
                                        os.path.dirname(relative_path))

                if not os.path.exists(base_dir):
                    logging.debug('creating directory structure %s',
                                  os.path.join(local_wd,
                                               os.path.dirname(relative_path)))
                    os.makedirs(base_dir)

                logging.debug("copying %s to %s",
                              source, os.path.join(local_wd, relative_path))

                destination = os.path.join(local_wd, relative_path)

                self.transfer_started(task=self.name,
                                      source=source,
                                      destination=destination)

                shutil.copyfile(source, destination)
            except OSError, e:
                self.transfer_failed(task=self.name,
                                     source=source,
                                     destination=destination,
                                     explanation=e.strerror)
            else:
                self.transfer_success(task=self.name,
                                      source=source,
                                      destination=destination)


class ExecutableTask(Task):

    '''Tasks that can be executed must provide this interface.'''

    @property
    def can_execute(self):
        return True


class Target(ExecutableTask):

    '''Class handling targets. Stores the info for executing them.'''

    def __init__(self,
                 name,
                 input,
                 output,
                 cores,
                 memory,
                 flags,
                 xsub,
                 code,
                 wd):
        # passing None as dependencies, 'cause Workflow will fill it in
        super(Target, self).__init__(name, None, wd)
        self.input = input
        self.output = output
        self.cores = cores
        self.memory = memory
        self.flags = flags
        self.submit_args = xsub
        self.submit = False
        self.code = code
        self.host = None

        # If set to submit, the target will be sent to the configured batch system as an
        # isolated job
        if self.submit_args != None:
            self.submit = True

        # Is changed, when the task is executed.
        self.local_wd = self.working_dir

        if 'dummy' in self.flags and len(self.output) > 0:
            print 'Target %s is marked as a dummy target but has output files.'
            print 'Dummy targets will never be run so cannot produce output!'
            sys.exit(2)
        self.is_dummy = 'dummy' in self.flags

    def get_input(self):
        for in_file, dependency in self.dependencies:
            # build path to file on remote source and destination
            relpath = os.path.relpath(in_file, self.working_dir)
            src_path = os.path.join(dependency.local_wd, relpath)

            dst_path = os.path.join(self.local_wd, relpath)

            # figure out source and destination hosts
            src_host = dependency.host
            dst_host = self.host

            # if the dependency was checkpointed, we check if the file exists
            # on the node at which it was generated. If not, we should fetch
            # the file from remote storage.
            if dependency.checkpoint and not dependency.host:
                src_host = dst_host
                src_path = in_file

                if local('stat {0} &> /dev/null'.format(src_path)) < 0:
                    logging.error('''output of checkpointed
                                  dependency does not exist at %s --
                                  halting''' %
                                  src_path)
                    os.exit(1)

            remote('mkdir -p {0}'.format(os.path.dirname(dst_path)), dst_host)

            # if the source host is the same as the destination host, we won't
            # copy any files, but just make a hardlink to the source file.
            if src_host == dst_host and not dependency.checkpoint:
                self.transfer_started(task=self.name,
                                      source=src_path,
                                      destination=dst_path)

                errorcode = remote('ln {0} {1}'.format(src_path, dst_path),
                                   src_host)

                if errorcode > 0:
                    expl = 'could not hard link {0} to {1}'.format(src_path,
                                                                   dst_path)
                    self.transfer_failed(task=self.name,
                                         source=src_path,
                                         destination=dst_path,
                                         explanation=expl)
                else:
                    self.transfer_success(task=self.name,
                                          source=src_path,
                                          destination=dst_path)
            elif dependency.checkpoint:
                self.transfer_started(task=self.name,
                                      source=src_path,
                                      destination=dst_path)

                errorcode = remote('cp {0} {1}'.format(src_path, dst_path),
                                   src_host)

                if errorcode > 0:
                    expl = 'could not copy file {0} to {1}'.format(src_path,
                                                                   dst_path)
                    self.transfer_failed(task=self.name,
                                         source=src_path,
                                         destination=dst_path,
                                         explanation=expl)
                else:
                    self.transfer_success(task=self.name,
                                          source=src_path,
                                          destination=dst_path)                
            else:
                src = ''.join([src_host, ':', src_path])
                dst = ''.join([dst_host, ':', dst_path])

                self.transfer_started(task=self.name,
                                      source=src,
                                      destination=dst)

                if local('scp {0} {1}'.format(src, dst)) == 0:
                    self.transfer_success(task=self.name,
                                          source=src,
                                          destination=dst)
                else:
                    expl = 'could not copy file {0} to {1}'.format(src, dst)
                    self.transfer_failed(task=self.name,
                                         source=src,
                                         destination=dst,
                                         explanation=expl)

    def move_output(self, working_dir):
        for out_file in self.output:
            relpath = os.path.relpath(out_file, working_dir)

            # figure out where this task was run and its path at that host
            src_host = self.host
            src_path = os.path.join(self.local_wd, relpath)

            # make the directory which we're going to copy to
            local('mkdir -p {0}'.format(os.path.dirname(out_file)))

            src = ''.join([src_host, ':', src_path])

            self.transfer_started(task=self.name,
                                  source=src,
                                  destination=out_file)

            # now copy the file to the workflow working directory
            if remote('scp {0} {1}'.format(src, out_file), src_host) == 0:
                self.transfer_success(task=self.name,
                                      source=src,
                                      destination=out_file)
            else:
                expl = 'could not copy file {0} to {1}'.format(src, out_file)
                self.transfer_failed(task=self.name,
                                     source=src,
                                     destination=out_file,
                                     explanation=expl)

    @property
    def checkpoint(self):
        return 'checkpoint' in self.flags

    def set_checkpoint(self):
        if not 'checkpoint' in self.flags:
            self.flags.append('checkpoint')

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
                 path,
                 lists,
                 templates,
                 targets,
                 template_targets,
                 wd):
        self.path = path
        self.lists = lists
        self.templates = templates
        self.targets = targets
        self.template_targets = template_targets
        self.working_dir = wd

        self.target_names = []

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
