import os.path
import unittest

from integration import IntegrationTestCase, Sandbox


class OneTaskFailsTest(IntegrationTestCase):

    requirements = ['test_one_task_fails.gwf']

    def runTest(self):
        with Sandbox(self.requirements) as s:
            s.run('gwf -l test_one_task_fails.gwf')
            self.assertTrue(s.run('gwf-status | grep "failed"').find("failed"))


class MiddleTaskFailsTest(IntegrationTestCase):

    requirements = ['test_middle_task_fails.gwf']

    def runTest(self):
        with Sandbox(self.requirements) as s:
            s.run('gwf -l test_middle_task_fails.gwf')

            self.assertTrue(s.run('gwf-status -j 00001').find("TargetOne   C"))
            self.assertTrue(s.run('gwf-status -j 00001').find("TargetTwo   C"))
            self.assertTrue(s.run('gwf-status -j 00001').find("TargetThree F"))
            self.assertTrue(s.run('gwf-status -j 00001').find("TargetFour  F"))
            self.assertTrue(s.run('gwf-status -j 00001').find("TargetFive  F"))

if __name__ == '__main__':
    unittest.main()
