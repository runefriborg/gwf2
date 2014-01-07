from workflow import SystemFile, Target


def task_shape(task):
    '''Dispatching types of tasks to graphviz shapes...

    There's a tight coupling here that I'm not totally happy about,
    but the alternative is to have tasks contain code for giving the shapes.
    '''
    if isinstance(task, SystemFile):
        return 'note'

    if isinstance(task, Target):
        if len(task.input) == 0 and len(task.output) > 0:
            return 'invhouse'
        if len(task.output) == 0 and len(task.input) > 0:
            return 'house'
        return 'octagon'

    return 'box'


def print_node(node, out):
    '''Print the graphviz description of this node to "out".'''

    shape = 'shape = %s' % task_shape(node.task)

    print >> out, '\t"%s"' % node.name,
    print >> out, '[',
    print >> out, ','.join([shape, 'color = darkgreen']),
    print >> out, ']',
    print >> out, ';'

def print_group(group, out):
    '''Print the graphviz description of this group to "out".'''
    label = group.id + '___' + group.pos
    print >> out, 'subgraph "cluster_%s" {' % label
    print >> out, '\tlabel="%s";' % label
    for node in group.nodes:
        print_node(node, out)
    print >> out, '}'

def print_graphviz(graph, tasklist, groups, hide_boxes, out):
    '''Print the dependency graph in groups, to output stream out.'''

    print >> out, 'digraph workflow {'

    # Handle nodes
    if hide_boxes:
        for g in groups:
            for n in g.nodes:
                print_node(n, out)
    else:
        for g in groups:
            if g.nodes:
                print_group(g, out)

    # Handle edges
    for src in graph.nodes.values():

        # Skip graph node if not part of dependency path to specified targets
        if src.task not in tasklist:
            continue

        for fname, dst in src.dependencies:
            print >> out, '"%s"' % dst.name, '->', '"%s"' % src.name,
            print >> out, '[label="%s"]' % (fname),
            print >> out, ';'

    print >> out, '}'
