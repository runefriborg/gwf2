import os.path
import unittest

from integration import IntegrationTestCase, Sandbox


class OneToManyToOneTest(IntegrationTestCase):

    requirements = ['test_one_to_many_to_one.gwf']

    def runTest(self):
        with Sandbox(self.requirements) as s:
            s.run('gwf -l test_one_to_many_to_one.gwf')
            self.assertFileExists(os.path.join(s.sandbox_path, 'final'))

if __name__ == '__main__':
    unittest.main()
