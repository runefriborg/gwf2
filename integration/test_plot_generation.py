import os
import os.path
import unittest

from integration import IntegrationTestCase, Sandbox


class PlotGenerationTest(IntegrationTestCase):

    requirements = ['test_plot_generation.gwf', 'ponAbe2.fa.gz']

    def runTest(self):
        with Sandbox(self.requirements) as s:
            s.run('gwf -l test_plot_generation.gwf')

            job_id = os.listdir(os.path.join(s.sandbox_config_path, 'jobs'))[0]

            s.run('gwf-status -j {0} -p timeline'.format(job_id))
            self.assertFileExists(os.path.join(s.sandbox_path,
                                               'workflow_timeline.png'))

            s.run('gwf-status -j {0} -p scheduling'.format(job_id))
            self.assertFileExists(os.path.join(s.sandbox_path,
                                               'workflow_scheduling.png'))

if __name__ == '__main__':
    unittest.main()
