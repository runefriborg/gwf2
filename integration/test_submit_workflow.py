import os.path
import unittest

from integration import IntegrationTestCase
from gwf.process import local


def wait(command):
    out = local(command)
    ids = [line for line in out.splitlines() if line.endswith('.in')]
    local('qwait ' + ' '.join(ids))


class SubmitLinearWorkflowTest(IntegrationTestCase):

    requirements = ['test_chain_of_checkpointed_targets.gwf']

    def runTest(self):
        wait('gwf test_chain_of_checkpointed_targets.gwf')
        self.assertNotFileExists('file_one')
        self.assertFileExists('file_two')
        self.assertNotFileExists('file_three')
        self.assertFileExists('file_four')
        self.assertFileExists('file_five')


class SubmitOneToManyToOneWorkflowTest(IntegrationTestCase):

    requirements = ['test_one_to_many_to_one.gwf']

    def runTest(self):
        wait('gwf test_one_to_many_to_one.gwf')
        self.assertFileExists('final')

if __name__ == '__main__':
    unittest.main()
