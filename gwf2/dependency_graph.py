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


class SubmitGroup(object):

    '''A group of nodes to be scheduled together'''

    def __init__(self, id, pos, nodes):
        self.id = id
        self.pos = pos
        self.nodes = nodes
        self.submit_args = None

        # Dependencies are added at a finalize stage
        self.dependencies = set()

    def __repr__(self):
        return "<SubmitGroup id:'%s' pos:'%s' size:'%d' deps:%s>" % (self.id, self.pos, len(self.nodes), str(self.dependencies))


class DependencyGraph(object):

    '''A complete dependency graph, with code for scheduling a workflow.'''

    def __init__(self, workflow):
        self.workflow = workflow
        self.nodes = dict()
        for name, target in workflow.targets.items():
            if name not in self.nodes:
                self.nodes[name] = self.build_DAG(target)

    @property
    def end_targets(self):
        '''Find targets which are not depended on by any other target.'''
        candidates = self.nodes.values()
        for _, node in self.nodes.items():
            for _, dependency in node.dependencies:
                if dependency in candidates:
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

    def update_reference_counts(self):
        roots = [self.nodes[target_name]
                 for target_name in self.workflow.target_names]

        # reset (set to 1, as the on_task_done function also gets a reference)
        for node in self.nodes.values():
            node.task.references = 1

        # count
        def dfs(node):
            for _, dep in node.dependencies:
                dep.task.references += 1
                dfs(dep)
        for root in roots:
            dfs(root)


    def tasklist(self, target_names):
        roots = [self.nodes[target_name] for target_name in target_names]
        processed = set()
        tasks = []

        def dfs(node):
            if node in processed or not node.task.can_execute:
                return

            processed.add(node)
            tasks.append(node.task)

            for _, dep in node.dependencies:
                dfs(dep)

        for root in roots:
            dfs(root)

        # Put tasks in optimal order
        tasks.reverse()

        return tasks

    def split_workflow(self, target_names):
        '''
            Splits the workflow into groups and returns SubmitGroups.
        '''

        roots = [self.nodes[target_name] for target_name in target_names]

        # Temporary group structure for depth-first traversal (dfs)
        class Group(object):
            nodes = []

            def extract_free_from(self, submit_node):
                ok = []
                def test_dep(node):
                    # Test dependency
                    for _, dep in node.dependencies:
                        if submit_node == dep:
                            return True

                    # Next dependency level (recursive)
                    for _, dep in node.dependencies:
                        if test_dep(dep):
                            return True

                    # Node does not depend on submit_node
                    return False

                for n in self.nodes:
                    if not test_dep(n):
                        ok.append(n)

                for n in ok:
                    self.nodes.remove(n)

                return ok

            def add(self, node):
                self.nodes.append(node)

            def extract(self):
                g = self.nodes
                self.nodes = []
                return g


        # Temporary group cluster structure for depth-first traversal (dfs)
        class GroupCluster(object):
            groups = []
            mapNodeToGroup = {}

            def add(self, id, pos, nodes):
                s = SubmitGroup(id, pos, nodes)

                # Add arguments for scheduler
                if pos=='task':
                    s.submit_args = nodes[0].task.submit_args

                self.groups.append(s)
                for n in s.nodes:
                    self.mapNodeToGroup[n] = s

            def finalize(self):
                """
                    All dependencies between SubmitGroups are finalized and the result is returned.
                """

                # Use mapNodetoGroup to update dependencies
                for node in self.mapNodeToGroup:
                    for _, dep in node.dependencies:
                        if dep.task.can_execute:
                            if self.mapNodeToGroup[node] != self.mapNodeToGroup[dep]:
                                self.mapNodeToGroup[node].dependencies.add(self.mapNodeToGroup[dep])

                                # set dependency node to checkpoint to force output to shared disk
                                dep.task.set_checkpoint()

                return self.groups


        section = GroupCluster()
        current = Group()
        processed = set()

        # Run depth-first traversal
        def dfs(node):
            if not node.task.can_execute:
                return

            if node in processed:
                return

            processed.add(node)

            if node.task.submit:
                # Find tasks in current.group, which does not depend on node
                section.add(node.task.name, "inbetween", current.extract_free_from(node))

                # Tasks to run after node
                section.add(node.task.name, "after", current.extract())

                # node
                section.add(node.task.name, "task", [node])

                for _, dep in node.dependencies:
                    dfs(dep)

                # Tasks to run before node
                section.add(node.task.name, "before", current.extract())

            else:
                current.add(node)

                for _, dep in node.dependencies:
                    dfs(dep)


        for root in roots:
            dfs(root)

        section.add('', 'inbetween', current.extract())

        return section.finalize()


    def schedule(self, target_names):
        """
        Arrange the tasks in the workflow into groups of tasks, based on which tasks
        have the submit flag enabled.

        For each task with a submit flag, devide the set into four groups..
          * The tasks before the submit task
          * The tasks after the submit task
          * The task
          * The in-between tasks (this is a greedy group)

        """

        roots = [self.nodes[target_name] for target_name in target_names]
        end_targets = self.workflow.target_names

        def files_exist(files):
            return all(map(_file_exists, files))

        def is_oldest(task):
            age_of_oldest_output_file = age_of_oldest_file(task.output)

            def dfs(node_task):
                if node_task.checkpoint or node_task.name in end_targets:
                    if not files_exist(node_task.output):
                        return False
                    if age_of_newest_file(node_task.output) > age_of_oldest_output_file:
                        return False
                return all(dfs(dependency)
                           for _, dependency in node_task.dependencies)

            return dfs(task)

        processed = set()
        schedule = []

        groups = [[]]

        def dfs(node, g):
            if node in processed or not node.task.can_execute:
                return

            if node.task.checkpoint or node.task.name in end_targets:
                if files_exist(node.task.output) and is_oldest(node.task):
                    return


            if node.task.submit:
                # make two new groups
                g.append([node.task])
                g.append(g[0])
                g[0] = [[]]
            else:
                # old group
                g[0].append(node.task)


            schedule.append(node.task)
            processed.add(node)

            for _, dep in node.dependencies:
                dfs(dep, g)

        for root in roots:
            dfs(root, groups)

        #for g in groups:
        #    print "Group"
        #    for t in g:
        #        print "\t" + str(t)

        # Put tasks in optimal order
        schedule.reverse()

        return schedule
