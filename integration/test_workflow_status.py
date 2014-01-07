import os.path
import unittest

from integration import IntegrationTestCase, Sandbox


class OneTaskFailsTest(IntegrationTestCase):

    requirements = ['test_one_task_fails.gwf']

    def runTest(self):
        with Sandbox(self.requirements) as s:
            s.run('gwf -l test_one_task_fails.gwf')
            self.assertTrue(s.run('gwf-status').find("failed") > -1)


class MiddleTaskFailsTest(IntegrationTestCase):

    requirements = ['test_middle_task_fails.gwf']

    def runTest(self):
        with Sandbox(self.requirements) as s:
            s.run('gwf -l test_middle_task_fails.gwf')
            self.assertTrue(s.run('gwf-status --no-color -j 00001').find("TargetOne   C") > -1)
            self.assertTrue(s.run('gwf-status --no-color -j 00001').find("TargetTwo   C") > -1)
            self.assertTrue(s.run('gwf-status --no-color -j 00001').find("TargetThree F") > -1)


class StatusDuringRunTest(IntegrationTestCase):

    requirements = ['test_task_two_of_three_fails.gwf']

    def runTest(self):
        import time
        import multiprocessing

        with Sandbox(self.requirements) as s:
            def test_status():
                # wait for the first task to finish
                while not os.path.exists(os.path.join(s.sandbox_scratch_path, '00001', 'TargetOne', 'file_one')):
                    time.sleep(0.1)

                # when the first task has produced file_one, we sleep a bit
                # to let TargetTwo start
                time.sleep(2)

                # then check if the status of TargetTwo is "running"
                self.assertTrue(s.run('gwf-status --no-color -j 00001').find('TargetTwo R') > -1)

                # wait 10 seconds. TargetTwo should then have failed and
                # TargetThree should be running.
                time.sleep(10)
                self.assertTrue(s.run('gwf-status --no-color -j 00001').find('TargetOne C') > -1)
                self.assertTrue(s.run('gwf-status --no-color -j 00001').find('TargetTwo F') > -1)

            process = multiprocessing.Process(target=test_status, args=())
            process.start()
            s.run('gwf -l test_task_two_of_three_fails.gwf')
            process.join()

if __name__ == '__main__':
    unittest.main()
