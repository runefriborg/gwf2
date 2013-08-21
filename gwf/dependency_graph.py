'''Graph for dependency relationships of targets.'''


class Node(object):

    '''A node in the dependencies DAG.'''

    def __init__(self, name, task, dependencies):
        self.name = name
        self.task = task
        self.dependencies = dependencies

        # The node needs to be run if what it contains needs to be run
        # or any of the upstream nodes need to be run...
        self.should_run = self.task.should_run


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
        '''Linearize the targets to be run.

        Returns a list of tasks to be run (in the order they should run or
        be submitted to the cluster to make sure dependences are handled
        correctly) and a set of the names of tasks that will be scheduled
        (to make sure dependency flags are set in the qsub command).

        '''

        root = self.nodes[target_name]

        processed = set()
        scheduled = set()
        schedule = []

        def dfs(node):
            if node in processed:
                return

            # If this task needs to run, then schedule it
            if node.should_run:
                # schedule all dependencies before we schedule this task
                for _, dep in node.dependencies:
                    dfs(dep)
                schedule.append(node)
                scheduled.add(node.name)

            processed.add(node)

        dfs(root)
        return schedule, scheduled
