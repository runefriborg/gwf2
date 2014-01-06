import os.path
import unittest

from integration import IntegrationTestCase, Sandbox


class OneTargetOfMultipleTest(IntegrationTestCase):

    requirements = ['test_one_target_of_multiple.gwf']

    def runTest(self):
        with Sandbox(self.requirements) as s:
            s.run('gwf -l -t SinkOne test_one_target_of_multiple.gwf')
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_a'))
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_b'))
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_c'))

        with Sandbox(self.requirements) as s:
            s.run('gwf -l -t SinkTwo test_one_target_of_multiple.gwf')
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_a'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_b'))
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_c'))

        with Sandbox(self.requirements) as s:
            s.run('gwf -l -t SinkThree test_one_target_of_multiple.gwf')
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_a'))
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_b'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_c'))

        with Sandbox(self.requirements) as s:
            s.run('gwf -l -t SinkOne SinkTwo test_one_target_of_multiple.gwf')
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_a'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_b'))
            self.assertNotFileExists(os.path.join(s.sandbox_path, 'final_c'))

        with Sandbox(self.requirements) as s:
            s.run('gwf -l -a test_one_target_of_multiple.gwf')
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_a'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_b'))
            self.assertFileExists(os.path.join(s.sandbox_path, 'final_c'))

if __name__ == '__main__':
    unittest.main()
