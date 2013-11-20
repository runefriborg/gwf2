import os
import os.path
import unittest

import subprocess
import tempfile
import shutil

TEMP_PREFIX = 'gwf_integration_'


class Sandbox(object):
    def __init__(self, requirements):
        self.sandbox_path = tempfile.mkdtemp(prefix=TEMP_PREFIX)
        self.sandbox_scratch_path = tempfile.mkdtemp(prefix=TEMP_PREFIX)
        self.sandbox_config_path = tempfile.mkdtemp(prefix=TEMP_PREFIX)

        for requirement in requirements:
            shutil.copy(requirement, self.sandbox_path)

        self.old_working_dir = os.getcwd()
        os.chdir(self.sandbox_path)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        shutil.rmtree(self.sandbox_path)
        shutil.rmtree(self.sandbox_scratch_path)
        shutil.rmtree(self.sandbox_config_path)

        os.chdir(self.old_working_dir)

    def run(self, command):
        try:
            process = subprocess.call(command,
                                      shell=True,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
        except subprocess.CalledProcessError, e:
            return (process.stdout, process.stderr, e.returncode)


class IntegrationTestCase(unittest.TestCase):

    def assertFileExists(self, *args):
        for arg in args:
            self.assertTrue(os.path.exists(arg), '%s must exist' % arg)

    def assertNotFileExists(self, *args):
        for arg in args:
            self.assertFalse(os.path.exists(arg), '%s must not exist' % arg)
