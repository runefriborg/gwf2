import os.path
import unittest

from integration import IntegrationTestCase, Sandbox


class ChainOfCheckpointedTargetsTest(IntegrationTestCase):

    # just use some workflow file...
    requirements = ['test_chain_of_checkpointed_targets.gwf']

    def runTest(self):
        with Sandbox(self.requirements) as s:
            s.run('gwf -l test_chain_of_checkpointed_targets.gwf')
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'file_one'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'file_two'))
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'file_three'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'file_four'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'file_five'))

if __name__ == '__main__':
    unittest.main()
