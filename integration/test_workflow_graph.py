import os.path
import unittest

from integration import IntegrationTestCase, Sandbox


class WorkflowGraphTest(IntegrationTestCase):

    # just use some workflow file...
    requirements = ['test_one_target_of_multiple.gwf']

    def runTest(self):
        with Sandbox(self.requirements) as s:
            s.run('gwf-workflow-graph test_one_target_of_multiple.gwf')
            self.assertFileExists(os.path.join(s.sandbox_path, 'test_one_target_of_multiple.gwf.dot'))
            s.run('dot -Tpng test_one_target_of_multiple.gwf.dot > test.png')
            self.assertFileExists(os.path.join(s.sandbox_path, 'test.png'))

if __name__ == '__main__':
    unittest.main()
