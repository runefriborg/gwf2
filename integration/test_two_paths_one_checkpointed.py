import unittest
import os

from integration import IntegrationTestCase, Sandbox


class TwoPathsOneCheckpointedTest(IntegrationTestCase):

    requirements = ['test_two_paths_one_checkpointed.gwf']

    def runTest(self):
        with Sandbox(self.requirements) as s:
            s.touch('some_file_a')
            s.touch('some_file_b')

            s.run('gwf -l -f test_two_paths_one_checkpointed.gwf &> /dev/null')

            self.assertFileExists('some_file_aa', 'some_file_aa_bb')
            os.remove('some_file_aa_bb')

            s.run('gwf -l -f test_two_paths_one_checkpointed.gwf &> /dev/null')

            self.assertFileExists('some_file_aa', 'some_file_aa_bb')

if __name__ == '__main__':
    unittest.main()
