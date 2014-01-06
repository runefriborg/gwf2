import os.path
import unittest

from integration import IntegrationTestCase, Sandbox


class OneTargetOfMultipleTest(IntegrationTestCase):

    requirements = ['test_one_target_of_multiple.gwf']

    def runTest(self):
        with Sandbox(self.requirements) as s:
            s.run('gwf -l test_one_target_of_multiple.gwf -t SinkOne')
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_a'))
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_b'))
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_c'))

        with Sandbox(self.requirements) as s:
            s.run('gwf -l test_one_target_of_multiple.gwf -t SinkTwo')
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_a'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_b'))
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_c'))

        with Sandbox(self.requirements) as s:
            s.run('gwf -l test_one_target_of_multiple.gwf -t SinkThree')
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_a'))
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_b'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_c'))

        with Sandbox(self.requirements) as s:
            s.run('gwf -l test_one_target_of_multiple.gwf  -t SinkOne SinkTwo')
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_a'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_b'))
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_c'))

        with Sandbox(self.requirements) as s:
            s.run('gwf -l test_one_target_of_multiple.gwf -a')
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_a'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_b'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_c'))

if __name__ == '__main__':
    unittest.main()
