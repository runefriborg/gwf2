import os.path
import unittest

import subprocess

from integration import IntegrationTestCase


def wait(command):
    out = subprocess.check_output(command, shell=True)
    ids = [line for line in out.splitlines() if line.endswith('.in')]
    return subprocess.check_output('qwait ' + ' '.join(ids))


class SubmitLinearWorkflowTest(IntegrationTestCase):

    def runTest(self):
        wait('gwf test_chain_of_checkpointed_targets.gwf')
        self.assertNotFileExists('file_one')
        self.assertFileExists('file_two')
        self.assertNotFileExists('file_three')
        self.assertFileExists('file_four')
        self.assertFileExists('file_five')


class SubmitOneToManyToOneWorkflowTest(IntegrationTestCase):

    def runTest(self):
        wait('gwf test_one_to_many_to_one.gwf')
        self.assertFileExists('final')

if __name__ == '__main__':
    unittest.main()
