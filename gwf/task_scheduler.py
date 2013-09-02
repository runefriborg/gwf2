import subprocess
import logging

import reporting

from copy import copy

from dependency_graph import DependencyGraph
from process_scheduler import ProcessScheduler
from process import RemoteProcess, remote

from environment import env
from reporting import reporter


class TaskScheduler(object):

    def __init__(self, workflow):
        self.workflow = workflow

        # build the dependency graph
        self.dependency_graph = DependencyGraph(self.workflow)

        # For every target name specified by the user, compute its dependencies
        # and build a list of all tasks which must be run. self.schedule no
        # longer corresponds to the topological sorting of the tasks, but is
        # just a list of all tasks that must be run. The scheduler will figure
        # out the correct order to run them in.
        targets = [workflow.targets[target_name]
                   for target_name in workflow.target_names]

        self.schedule = set()
        for target in targets:
            schedule = self.dependency_graph.schedule(target.name)
            self.schedule.update(schedule)
        logging.debug('schedule: %s', ', '.join(
                      task.name for task in self.schedule))

        # Build a list of all the jobs that have not been completed yet.
        # Jobs should be removed from this list when they have completed.
        self.missing = copy(self.schedule)

        # This list contains all the running jobs.
        self.running = []

        reporter.report(reporting.WORKFLOW_STARTED,
                        file=self.workflow.path,
                        queued=[task.name for task in self.missing],
                        nodes=env.nodes.keys())

    def run(self):
        # ... then start the scheduler to actually run the jobs.
        self.scheduler = ProcessScheduler()
        self.scheduler.on('before', self.on_before_job_started)
        self.scheduler.on('started', self.on_job_started)
        self.scheduler.on('done', self.on_job_done)

        # Now, schedule everything that can be scheduled...
        self.schedule_tasks()
        self.scheduler.run()

    def schedule_tasks(self):
        '''Schedule all missing tasks.'''
        if not self.missing and not self.running:
            self.scheduler.stop()

            reporter.report(reporting.WORKFLOW_COMPLETED)
            reporter.finalize()

        # NOTE: The copy is IMPORTANT since we modify missing
        #       during scheduling.
        for task in copy(self.missing):
            self.schedule_task(task)

    def schedule_task(self, task):
        '''Schedule a single task if all dependencies have been computed'''
        logging.debug('scheduling task=%s', task.name)

        # skip dummy tasks that we shouldn't submit...
        if task.dummy or not task.can_execute:
            return

        # If all dependencies are done, we may schedule this task.
        for _, dep_task in task.dependencies:
            # If the dependency is not executable, we will not wait for it
            # to complete. Also, we will only wait for the dependency if it was
            # actually scheduled. If it wasn't scheduled, its output files
            # already exist and thus it should never be executed.
            if dep_task.can_execute and dep_task in self.schedule:
                if dep_task in self.schedule:
                    # if the dependency is either missing or still running,
                    # this task cannot be scheduled.
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
        logging.debug("running task=%s cores=%s cwd=%s code='%s'",
                      task.name, task.cores, task.local_wd, task.code.strip())

        task.host = self.get_available_node(task.cores)

        # decrease the number of cores that the chosen node has available
        env.nodes[task.host] -= task.cores

        logging.debug('making destination directory %s on host %s' %
                      (task.local_wd, task.host))
        remote('mkdir -p {0}'.format(task.local_wd), task.host)

        process = RemoteProcess(task.code.strip(),
                                task.host,
                                stderr=subprocess.STDOUT,
                                cwd=task.local_wd)

        self.scheduler.schedule(task, process)

    def on_before_job_started(self, task):
        self.missing.remove(task)

        # move all input files to local working directory
        logging.debug('fetching dependencies for %s' % task.name)
        task.get_input()

    def on_job_done(self, task, errorcode):
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
        host = task.host
        env.nodes[host] += task.cores

        self.running.remove(task)

        reporter.report(reporting.TASK_COMPLETED, task=task.name)
        logging.info('task done: %s', task.name)

        # reschedule now that we know that a task has finished
        self.schedule_tasks()

    def cleanup(self, task):
        if task.host:
            # delete the task directory on the host
            logging.debug('deleting directory %s on host %s' %
                          (task.local_wd, task.host))
            remote('rm -rf {0}'.format(task.local_wd), task.host)

    def on_job_started(self, task):
        self.running.append(task)

        reporter.report(reporting.TASK_STARTED,
                        task=task.name,
                        host=task.host,
                        working_dir=task.local_wd)

    def get_available_node(self, cores_needed):
        for node, cores in env.nodes.iteritems():
            if cores >= cores_needed:
                return node
