import os.path
import unittest

from integration import IntegrationTestCase
from gwf.process import local


class SubmitLinearWorkflowTest(IntegrationTestCase):

    requirements = ['test_chain_of_checkpointed_targets.gwf']

    def runTest(self):
        local('gwf test_chain_of_checkpointed_targets.gwf')
        self.assertNotFileExists('file_one')
        self.assertFileExists('file_two')
        self.assertNotFileExists('file_three')
        self.assertFileExists('file_four')
        self.assertFileExists('file_five')


class SubmitOneToManyToOneWorkflowTest(IntegrationTestCase):

    requirements = ['test_one_to_many_to_one.gwf']

    def runTest(self):
        local('gwf test_one_to_many_to_one.gwf')
        self.assertFileExists('final')

if __name__ == '__main__':
    unittest.main()
