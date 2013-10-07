import os
import os.path
import json
import time
import datetime
import shutil

WORKFLOW_QUEUED = 'WORKFLOW_QUEUED'
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
    WORKFLOW_QUEUED,
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

    The user feeds the :class:`ReportReader` with reports using :func:`write`
    and is then able to read the current state at any time.'''

    def __init__(self):
        # keeps track of the state of the workflow
        self.workflow = {
            'completed_at': None,
            'queued_at': None,
            'started_at': None,
            'file': None,
            'queued': [],
            'nodes': [],
            'failure': None,
            'tasks': {}
        }

        self.handlers = {
            WORKFLOW_QUEUED: self._workflow_queued,
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

    def write(self, report):
        timestamp, event, data = report
        self.handlers[event](
            datetime.datetime.fromtimestamp(timestamp), **data)

    def _workflow_queued(self, timestamp):
        self.workflow['queued_at'] = timestamp

    def _workflow_started(self, timestamp, file, queued, nodes):
        self.workflow['queued'] = queued
        self.workflow['nodes'] = nodes
        self.workflow['file'] = file
        self.workflow['started_at'] = timestamp

    def _workflow_completed(self, timestamp):
        self.workflow['completed_at'] = timestamp

    def _workflow_failed(self, timestamp, explanation):
        self.workflow['failure'] = {
            'timestamp': timestamp,
            'explanation': explanation
        }

    def _task_started(self, timestamp, task, host, working_dir):
        self.workflow['tasks'][task] = {
            'started_at': timestamp,
            'completed_at': None,
            'host': host,
            'working_dir': working_dir,
            'transfers': {},
            'failure': {}
        }

    def _task_completed(self, timestamp, task):
        self.workflow['tasks'][task]['completed_at'] = timestamp

    def _task_failed(self, task, timestamp, explanation):
        self.workflow['tasks'][task]['failure'] = {
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
        self.workflow['tasks'][task]['transfers'][(source, destination)] = transfer

    def _transfer_completed(self, timestamp, task, source, destination):
        key = (source, destination)
        self.workflow['tasks'][task]['transfers'][key]['completed_at'] = timestamp

    def _transfer_failed(self, timestamp, explanation,
                         task, source, destination):
        key = (source, destination)
        self.workflow['tasks'][task]['transfers'][key]['completed_at'] = timestamp
        self.workflow['tasks'][task]['transfers'][key]['failure'] = {
            'timestamp': timestamp,
            'explanation': explanation
        }


class FileLikeReportReader(ReportReader):
    '''A ReportReader which can be written to like a file.'''

    def write(self, report):
        super(FileLikeReportReader, self).write(json.loads(report))


class FileReportReader(ReportReader):
    '''Reads and interprets a report file to rebuild the state of the
    currently running job.'''

    def __init__(self, filename):
        super(FileReportReader, self).__init__()
        with open(filename) as f:
            for report in f.readlines():
                self.write(json.loads(report))


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
        # using shutil.move instead of os.rename to avoid
        # "Invalid cross-device link" exception.
        shutil.move(self.tmp_file, self.final_file)
