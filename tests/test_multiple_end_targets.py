import unittest
import os
import os.path

from gwf.parser import parse
from gwf.dependency_graph import DependencyGraph
testdir = os.path.dirname(__file__)


class DependencyGraphTest(unittest.TestCase):

    def test_auto_detection_of_end_targets(self):
        path = os.path.join(testdir, 'multiple_end_targets.gwf')

        workflow = parse(path, [], True)
        DependencyGraph(workflow)

        self.assertEquals(set(workflow.target_names),
                          set(['TargetB', 'TargetC']))
