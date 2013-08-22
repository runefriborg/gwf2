'''Graph for dependency relationships of targets.'''

import os
import os.path
import time


def _file_exists(fname):
    return os.path.exists(fname)


def _get_file_timestamp(fname):
    try:
        return os.path.getmtime(fname)
    except:
        return time.time()


def age_of_newest_file(files):
    youngest_output_time = 0
    for filename in files:
        output_time = _get_file_timestamp(filename)
        if output_time > youngest_output_time:
            youngest_output_time = output_time
    return youngest_output_time


def age_of_oldest_file(files):
    oldest_output_time = time.time()
    for filename in files:
        output_time = _get_file_timestamp(filename)
        if output_time < oldest_output_time:
            oldest_output_time = output_time
    return oldest_output_time


def _make_absolute_path(working_dir, fname):
    if os.path.isabs(fname):
        abspath = fname
    else:
        abspath = os.path.join(working_dir, fname)
    return os.path.normpath(abspath)


class Node(object):

    '''A node in the dependencies DAG.'''

    def __init__(self, name, task, dependencies):
        self.name = name
        self.task = task
        self.dependencies = dependencies


class DependencyGraph(object):

    '''A complete dependency graph, with code for scheduling a workflow.'''

    def __init__(self, workflow):
        self.workflow = workflow
        self.nodes = dict()
        for name, target in workflow.targets.items():
            if name not in self.nodes:
                self.nodes[name] = self.build_DAG(target)

        # If all end targets should be run, we have to figure out what those
        # are and set the target names in the workflow.
        if self.workflow.run_all:
            self.workflow.target_names = \
                set(node.task.name for node in self.end_targets())

        self.count_references()

    def end_targets(self):
        '''Find targets which are not depended on by any other target.'''
        candidates = self.nodes.values()
        for _, node in self.nodes.items():
            for _, dependency in node.dependencies:
                if dependency in candidates \
                        or not dependency.task.can_execute:
                    candidates.remove(dependency)
        return candidates

    def add_node(self, task, dependencies):
        '''Create a graph node for a workflow task, assuming that its
        dependencies are already wrapped in nodes. The type of node
        depends on the type of task.

        Here we call the objects from the workflow for "tasks" and the
        nodes in the dependency graph for "nodes" and we represent each
        "task" with one "node", keeping the graph structure captured by
        nodes in the dependency DAG and the execution logic in the
        objects they refer to.'''

        node = Node(task.name, task, dependencies)
        self.nodes[task.name] = node
        return node

    def build_DAG(self, task):
        '''Run through all the dependencies for "target" and build the
        nodes that are needed into the graph, returning a new node with
        dependencies.

        Here we call the objects from the workflow for "tasks" and the
        nodes in the dependency graph for "nodes" and we represent each
        "task" with one "node", keeping the graph structure captured by
        nodes in the dependency DAG and the execution logic in the
        objects they refer to.'''

        def dfs(task):
            if task.name in self.nodes:
                return self.get_node(task.name)
            else:
                deps = [(fname, dfs(dep)) for fname, dep in task.dependencies]
                return self.add_node(task, deps)

        return dfs(task)

    def has_node(self, name):
        return name in self.nodes

    def get_node(self, name):
        return self.nodes[name]

    def count_references(self):
        roots = [self.nodes[target_name]
                 for target_name in self.workflow.target_names]

        def dfs(node, root):
            if node != root:
                node.task.references += 1
            for _, dep in node.dependencies:
                dfs(dep, root)
        for root in roots:
            dfs(root, root)

    def schedule(self, target_name):
        root = self.nodes[target_name]
        end_targets = self.workflow.target_names

        def files_exist(files):
            return all(map(_file_exists, files))

        def is_oldest(task):
            age_of_oldest_output_file = age_of_oldest_file(task.output)

            def dfs(root):
                if root.checkpoint or root.name in end_targets:
                    if not files_exist(root.output):
                        return False
                    if age_of_newest_file(root.output) > age_of_oldest_output_file:
                        return False
                return all(dfs(dependency)
                           for _, dependency in root.dependencies)

            return dfs(task)

        processed = set()
        schedule = []

        def dfs(node):
            if node in processed or not node.task.can_execute:
                return

            if node.task.checkpoint or node.task.name in end_targets:
                if files_exist(node.task.output) and is_oldest(node.task):
                    return

            schedule.append(node.task)
            processed.add(node)

            for _, dep in node.dependencies:
                dfs(dep)

        dfs(root)
        return schedule
