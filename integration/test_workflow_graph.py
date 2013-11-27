import os.path
import unittest

from integration import IntegrationTestCase, Sandbox


class WorkflowGraphTest(IntegrationTestCase):

    # just use some workflow file...
    requirements = ['test_one_target_of_multiple.gwf']

    def runTest(self):
        with Sandbox(self.requirements) as s:
            s.run('gwf-workflow-graph -f test_one_target_of_multiple.gwf')
            self.assertFileExists(os.path.join(s.sandbox_path, 'workflow.dot'))

if __name__ == '__main__':
    unittest.main()
