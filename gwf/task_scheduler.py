import os
import subprocess
import logging

from copy import copy

from dependency_graph import DependencyGraph
from process_scheduler import ProcessScheduler
from process import RemoteProcess, remote

PBS_NODEFILE = os.environ['PBS_NODEFILE']


class TaskScheduler(object):

    def __init__(self, workflow):
        self.workflow = workflow

        # build the dependency graph
        self.dependency_graph = DependencyGraph(self.workflow)

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
        target = workflow.targets[workflow.target_name]
        self.schedule, self.scheduled_tasks = \
            self.dependency_graph.schedule(target.name)

        # Build a list of all the jobs that have not been completed yet.
        # Jobs should be removed from this list when they have completed.
        self.missing = [job.task for job in self.schedule]

        # This list contains all the running jobs.
        self.running = []

    def run(self):
        # ... then start the scheduler to actually run the jobs.
        self.scheduler = ProcessScheduler()
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

    def on_before_job_started(self, task):
        self.missing.remove(task)

        # move all input files to local working directory
        logging.info('fetching dependencies for %s' % task.name)
        task.get_input()

    def on_job_done(self, task, errorcode):
        if errorcode > 0:
            logging.error(
                'task %s stopped with non-zero error code %s - halting',
                task.name, errorcode)
            self.scheduler.stop()

        # if this task is the final task, we should copy its output files to
        # the the workflow directory.
        if task.name == self.workflow.target_name or task.checkpoint:
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
