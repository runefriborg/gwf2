import os
import os.path
import json
import time
import datetime

WORKFLOW_STARTED = 'WORKFLOW_STARTED'
WORKFLOW_COMPLETED = 'WORKFLOW_COMPLETED'
WORKFLOW_FAILED = 'WORKFLOW_FAILED'
TASK_STARTED = 'TASK_STARTED'
TASK_COMPLETED = 'TASK_COMPLETED'
TASK_FAILED = 'TASK_FAILED'
TRANSFER_STARTED = 'TRANSFER_STARTED'
TRANSFER_COMPLETED = 'TRANSFER_COMPLETED'
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


class Reporter(object):

    def report(self, event, data):
        pass

    def finalize(self):
        pass


class ReportReader(object):
    '''Reads and interprets a stream of reports to rebuild the state of the
    currently running job.

    The user feeds the :class:`ReportReader` with reports using :func:`feed`
    and is then able to read the current state at any time.'''

    def __init__(self):
        # the filename of the workflow file which initiated this job
        self.file = None

        # keeps track of the state of the workflow
        self.workflow = {
            'completed_at': None,
            'started_at': None,
            'file': None,
            'queued': [],
            'nodes': [],
            'failure': None
        }

        self.tasks = {}

        self.handlers = {
            WORKFLOW_STARTED: self._workflow_started,
            WORKFLOW_COMPLETED: self._workflow_completed,
            WORKFLOW_FAILED: self._workflow_failed,
            TASK_STARTED: self._task_started,
            TASK_COMPLETED: self._task_completed,
            TASK_FAILED: self._task_failed,
            TRANSFER_STARTED: self._transfer_started,
            TRANSFER_COMPLETED: self._transfer_completed,
            TRANSFER_FAILED: self._transfer_failed
        }

    def feed(self, report):
        timestamp, event, data = report
        self.handlers[event](
            datetime.datetime.fromtimestamp(timestamp), **data)

    def _workflow_started(self, timestamp, file, queued, nodes):
        self.workflow['queued'] = queued
        self.workflow['nodes'] = nodes
        self.workflow['file'] = file
        self.workflow['started_at'] = timestamp

    def _workflow_completed(self, timestamp):
        self.workflow['completed_at'] = timestamp

    def _workflow_failed(self, timestamp, explanation):
        self.workflow['failed'] = {
            'timestamp': timestamp,
            'explanation': explanation
        }

    def _task_started(self, timestamp, task, host, working_dir):
        self.tasks[task] = {
            'started_at': timestamp,
            'completed_at': None,
            'host': host,
            'working_dir': working_dir,
            'transfers': {},
            'failure': {}
        }

    def _task_completed(self, timestamp, task):
        self.tasks[task]['completed_at'] = timestamp

    def _task_failed(self, task, timestamp, explanation):
        self.tasks[task]['failure'] = {
            'timestamp': timestamp,
            'explanation': explanation
        }

    def _transfer_started(self, timestamp, task, source, destination):
        transfer = {
            'started_at': timestamp,
            'completed_at': None,
            'source': source,
            'destination': destination,
            'failure': {}
        }
        self.tasks[task]['transfers'][(source, destination)] = transfer

    def _transfer_completed(self, timestamp, task, source, destination):
        key = (source, destination)
        self.tasks[task]['transfers'][key]['completed_at'] = timestamp

    def _transfer_failed(self, timestamp, explanation,
                         task, source, destination):
        key = (source, destination)
        self.tasks[task]['transfers'][key]['failure'] = {
            'timestamp': timestamp,
            'explanation': explanation
        }


class FileLikeReportReader(ReportReader):
    '''Reads and interprets a report stream read from a file-like
    object to rebuild the state of the currently running object.'''

    def __init__(self, file_like):
        super(FileLikeReportReader, self).__init__()
        for line in file_like.readlines():
            self.feed(json.loads(line))


class FileReportReader(FileLikeReportReader):
    '''Reads and interprets a report file to rebuild the state of the
    currently running job.'''

    def __init__(self, filename):
        with open(filename) as f:
            super(FileReportReader, self).__init__(f)


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
