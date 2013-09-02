import os
import os.path
import json
import time

from environment import env

WORKFLOW_STARTED = 'WORKFLOW_STARTED'
WORKFLOW_COMPLETED = 'WORKFLOW_COMPLETED',
WORKFLOW_FAILED = 'WORKFLOW_FAILED',
TASK_STARTED = 'TASK_STARTED',
TASK_COMPLETED = 'TASK_COMPLETED',
TASK_FAILED = 'TASK_FAILED',
TRANSFER_STARTED = 'TRANSFER_STARTED',
TRANSFER_COMPLETED = 'TRANSFER_COMPLETED',
TRANSFER_FAILED = 'TRANSFER_FAILED'

EVENT_TYPES = {
    WORKFLOW_STARTED,
    WORKFLOW_COMPLETED,
    WORKFLOW_FAILED,
    TASK_STARTED,
    TASK_COMPLETED,
    TASK_FAILED,
    TRANSFER_STARTED,
    TRANSFER_COMPLETED,
    TRANSFER_FAILED
}

LOG_NAME = 'log'

reporter = None


class Reporter(object):

    def report(self, event, data):
        pass

    def finalize(self):
        pass


class FileReporter(Reporter):

    def __init__(self, tmp_dir, final_dir):
        self.tmp_file = os.path.join(tmp_dir, LOG_NAME)
        self.final_file = os.path.join(final_dir, LOG_NAME)

        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
        if not os.path.exists(final_dir):
            os.makedirs(final_dir)

    def report(self, event, **data):
        if not event in EVENT_TYPES:
            raise Exception('event %s not supported.')

        with open(self.tmp_file, 'a') as f:
            json.dump((time.time(), event, data), f,
                      separators=(',', ':'))
            f.write('\n')

    def finalize(self):
        os.rename(self.tmp_file, self.final_file)

if not reporter:
    # Initialize file reporter such that it writes the log file to local
    # storage (scratch) until the reporter is finalized.
    tmp_dir = os.path.join(env.config_dir, 'jobs', env.job_id)
    final_dir = os.path.join(env.config_dir, 'jobs', env.job_id)
    reporter = FileReporter(tmp_dir=tmp_dir,
                            final_dir=final_dir)

__all__ = ['reporter']
