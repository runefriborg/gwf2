import os.path
import subprocess
import logging

import reporting

from copy import copy

from dependency_graph import DependencyGraph
from process import RemoteProcess, remote


class TaskScheduler(object):

    def __init__(self, environment, reporter, workflow, scheduler):
        self.environment = environment
        self.reporter = reporter
        self.workflow = workflow
        self.scheduler = scheduler

        self.job_dir = os.path.join(self.environment.scratch_dir,
                                    self.environment.job_id)

        # build the dependency graph
        self.graph = DependencyGraph(self.workflow)

        # For every target name specified by the user, compute its dependencies
        # and build a list of all tasks which must be run. self.schedule no
        # longer corresponds to the topological sorting of the tasks, but is
        # just a list of all tasks that must be run. The scheduler will figure
        # out the correct order to run them in.
        targets = [workflow.targets[target_name]
                   for target_name in workflow.target_names]

        self.schedule = frozenset(*(self.graph.schedule(target.name)
                                    for target in targets))

        # Build a list of all the jobs that have not been completed yet.
        # Jobs should be removed from this list when they have completed.
        self.missing = set(self.schedule)

        # This list contains all the running jobs.
        self.running = set()

        self.reporter.report(reporting.WORKFLOW_STARTED,
                             file=self.workflow.path,
                             queued=[task.name for task in self.missing],
                             nodes=self.environment.nodes.keys())

    def run(self):
        self.scheduler.before += self.on_before_task_started
        self.scheduler.started += self.on_task_started
        self.scheduler.done += self.on_task_done
        self.scheduler.stopped += self.on_workflow_stopped

        # Now, schedule everything that can be scheduled...
        self.schedule_tasks()
        self.scheduler.run()

    def schedule_tasks(self):
        '''Schedule all missing tasks.'''
        if not self.missing and not self.running:
            self.scheduler.stop()

        # NOTE: The copy is IMPORTANT since we modify missing
        #       during scheduling.
        for task in copy(self.missing):
            self.schedule_task(task)

    def _dependencies_done(self, task):
        # If a dependency is not executable, we will not wait for it
        # to complete. Also, we will only wait for the dependency if it was
        # actually scheduled. If it wasn't scheduled, its output files
        # already exist and thus it should never be executed.
        for _, dependency in task.dependencies:
            if dependency.can_execute and dependency in self.schedule:
                if dependency in self.missing or dependency in self.running:
                    return False
        return True

    def schedule_task(self, task):
        '''Schedule a single task if all dependencies have been computed'''
        logging.debug('scheduling task=%s', task.name)

        # skip dummy tasks that we shouldn't submit...
        if task.dummy or not task.can_execute:
            return

        # If all dependencies are done, we may schedule this task.
        if not self._dependencies_done(task):
            return

        task.local_wd = os.path.join(self.job_dir, task.name)

        # schedule the task
        logging.debug("running task=%s cores=%s cwd=%s code='%s'",
                      task.name, task.cores, task.local_wd, task.code.strip())

        task.host = self.get_available_node(task.cores)

        # decrease the number of cores that the chosen node has available
        self.environment.nodes[task.host] -= task.cores

        # TODO: move this in to some kind of FileRegistry...
        logging.debug('making destination directory %s on host %s' %
                      (task.local_wd, task.host))
        remote('mkdir -p {0}'.format(task.local_wd), task.host)

        task.stderr = open(os.path.join(self.job_dir,
                                        task.name + '.stderr'), 'w')
        task.stdout = open(os.path.join(self.job_dir,
                                        task.name + '.stdout'), 'w')

        process = RemoteProcess(task.code.strip(),
                                task.host,
                                cwd=task.local_wd,
                                stderr=task.stderr,
                                stdout=task.stdout)

        self.scheduler.schedule(task, process)

    def on_before_task_started(self, task):
        self.missing.remove(task)

        task.transfer_started += self.on_transfer_started
        task.transfer_success += self.on_transfer_success
        task.transfer_failed += self.on_transfer_failed

        # move all input files to local working directory
        logging.debug('fetching dependencies for %s' % task.name)
        task.get_input()

    def on_task_done(self, task, errorcode):
        task.stdout.close()
        task.stderr.close()
        if errorcode > 0:
            logging.error(
                'task %s stopped with non-zero error code %s - halting',
                task.name, errorcode)
            self.scheduler.stop()

        # if this task is the final task, we should copy its output files to
        # the the workflow directory.
        if task.name in self.workflow.target_names or task.checkpoint:
            task.move_output(self.workflow.working_dir)

        # decrease references for all dependencies of this task. Cleanup will
        # automatically be run for the dependency if its reference count is 0.
        for _, dependency in task.dependencies:
            if not dependency.can_execute:
                continue
            dependency.references -= 1
            if dependency.references == 0:
                self.cleanup(dependency)

        # figure out where this task was run and increment the number of cores
        # available on the host, since the job is now done.
        self.environment.nodes[task.host] += task.cores

        self.running.discard(task)

        task.transfer_started -= self.on_transfer_started
        task.transfer_success -= self.on_transfer_success
        task.transfer_failed -= self.on_transfer_failed

        self.reporter.report(reporting.TASK_COMPLETED, task=task.name)
        logging.info('task done: %s', task.name)

        # reschedule now that we know that a task has finished
        self.schedule_tasks()

    def on_transfer_started(self, *args, **kwargs):
        self.reporter.report(reporting.TRANSFER_STARTED, *args, **kwargs)

    def on_transfer_success(self, *args, **kwargs):
        self.reporter.report(reporting.TRANSFER_COMPLETED, *args, **kwargs)

    def on_transfer_failed(self, *args, **kwargs):
        self.reporter.report(reporting.TRANSFER_FAILED, *args, **kwargs)

    def cleanup(self, task):
        if task.host:
            # delete the task directory on the host
            logging.debug('deleting directory %s on host %s' %
                          (task.local_wd, task.host))
            remote('rm -rf {0}'.format(task.local_wd), task.host)

    def on_task_started(self, task):
        self.running.add(task)

        self.reporter.report(reporting.TASK_STARTED,
                             task=task.name,
                             host=task.host,
                             working_dir=task.local_wd)

    def on_workflow_stopped(self):
        # Move log file from mother node to shared storage and somehow
        # indicate that the workflow logs have been moved.
        self.reporter.report(reporting.WORKFLOW_COMPLETED)
        self.reporter.finalize()

    def get_available_node(self, cores_needed):
        for node, cores in self.environment.nodes.iteritems():
            if cores >= cores_needed:
                return node
