# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Tests for ComposerTaskHandler"""

import io
import logging
import unittest
from unittest import mock

import grpc
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.logging_v2.types import ListLogEntriesRequest, ListLogEntriesResponse, LogEntry
from google.logging.type import log_severity_pb2
from google.protobuf import timestamp_pb2

from airflow.composer.composer_task_handler import ComposerTaskHandler
from airflow.composer.task_formatter import TaskFormatter
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone
from airflow.utils.state import State
from tests.test_utils.db import clear_db_runs


def _create_list_log_entries_response_mock(messages, token):
    dummy_timestamp = timestamp_pb2.Timestamp()
    dummy_timestamp.FromJsonString('2022-01-01T00:00:10+00:00')
    return ListLogEntriesResponse(
        entries=[
            LogEntry(
                timestamp=dummy_timestamp,
                text_payload=message,
                severity=log_severity_pb2.INFO,
                labels={'process': 'taskinstance.py:90'},
            )
            for message in messages
        ],
        next_page_token=token,
    )


def _remove_composer_handlers():
    for handler_ref in reversed(logging._handlerList[:]):
        handler = handler_ref()
        if not isinstance(handler, ComposerTaskHandler):
            continue
        logging._removeHandlerRef(handler_ref)
        del handler


class TestComposerLoggingHandlerTask(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        clear_db_runs()
        self.composerTaskHandler = ComposerTaskHandler()
        self.composerTaskHandler.ENVIRONMENT_NAME = 'composer-env'
        self.composerTaskHandler.ENVIRONMENT_LOCATION = 'loc'
        self.logger = logging.getLogger('logger')
        date = timezone.datetime(2022, 1, 1)
        self.dag = DAG('dag_for_testing_composer_task_handler', start_date=date)
        self.task = DummyOperator(task_id='task_for_testing_composer_log_handler', dag=self.dag)
        self.dagrun = self.dag.create_dagrun(
            state=State.RUNNING,
            run_id='test_composer_run_id',
            execution_date=date,
            data_interval=(date, date),
        )
        self.ti = self.dagrun.get_task_instance(task_id=self.task.task_id)
        self.ti.state = State.RUNNING
        self.ti.start_date = date
        self.ti.end_date = timezone.datetime(2022, 1, 3)
        self.ti.try_number = 1
        self.ti.hostname = 'default-hostname'
        self.addCleanup(_remove_composer_handlers)

    def tearDown(self):
        clear_db_runs()

    @mock.patch('airflow.composer.composer_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.composer.composer_task_handler.LoggingServiceV2Client')
    def test_should_read_logs_for_single_try(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock(['MSG1', 'MSG2'], None)]
        )
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        self.composerTaskHandler.read(self.ti, 1)

        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=['projects/project_id'],
                filter=(
                    'logName="projects/project_id/logs/airflow-worker"\n'
                    'resource.type="cloud_composer_environment"\n'
                    'resource.labels.project_id="project_id"\n'
                    'resource.labels.environment_name="composer-env"\n'
                    'resource.labels.location="loc"\n'
                    'timestamp>="2022-01-01T00:00:00+00:00"\n'
                    'timestamp<="2022-01-03T00:05:00+00:00"\n'
                    'labels.task-id="task_for_testing_composer_log_handler"\n'
                    'labels.workflow="dag_for_testing_composer_task_handler"\n'
                    'labels.execution-date="2022-01-01T00:00:00+00:00"\n'
                    'labels.try-number="1"\n'
                    'labels.worker_id="default-hostname"'
                ),
                order_by='timestamp asc',
                page_size=1000,
                page_token=None,
            )
        )

    @mock.patch('airflow.composer.composer_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.composer.composer_task_handler.LoggingServiceV2Client')
    def test_should_read_logs_for_all_tries(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        self.composerTaskHandler.read(self.ti)
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=['projects/project_id'],
                filter=(
                    'logName="projects/project_id/logs/airflow-worker"\n'
                    'resource.type="cloud_composer_environment"\n'
                    'resource.labels.project_id="project_id"\n'
                    'resource.labels.environment_name="composer-env"\n'
                    'resource.labels.location="loc"\n'
                    'timestamp>="2022-01-01T00:00:00+00:00"\n'
                    'timestamp<="2022-01-03T00:05:00+00:00"\n'
                    'labels.task-id="task_for_testing_composer_log_handler"\n'
                    'labels.workflow="dag_for_testing_composer_task_handler"\n'
                    'labels.execution-date="2022-01-01T00:00:00+00:00"\n'
                    'labels.worker_id="default-hostname"'
                ),
                order_by='timestamp asc',
                page_size=1000,
                page_token=None,
            )
        )

    @mock.patch('airflow.composer.composer_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.composer.composer_task_handler.LoggingServiceV2Client')
    def test_log_entries_are_formatted_in_expected_format(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock(['MSG1', 'MSG2'], None)]
        )
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        logs, metadata = self.composerTaskHandler.read(self.ti, 1)

        assert [
            (
                (
                    'default-hostname',
                    '*** Reading remote logs from Cloud Logging.\n'
                    '[2022-01-01 00:00:10+00:00] {taskinstance.py:90} INFO - MSG1\n'
                    '[2022-01-01 00:00:10+00:00] {taskinstance.py:90} INFO - MSG2',
                ),
            )
        ] == logs
        assert [{'end_of_log': True}] == metadata

    @mock.patch('airflow.composer.composer_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.composer.composer_task_handler.LoggingServiceV2Client')
    def test_should_read_logs_with_paging(self, mock_client, mock_get_creds_and_project_id):
        pages = [_create_list_log_entries_response_mock(['MSG1', 'MSG2'], 'token') for i in range(2)]
        pages.append(_create_list_log_entries_response_mock(['MSG1', 'MSG2'], None))
        mock_client.return_value.list_log_entries.return_value.pages = iter(pages)
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        logs, metadata = self.composerTaskHandler.read(self.ti, 1, {'download_logs': 'true'})

        assert [
            (
                (
                    'default-hostname',
                    '*** Reading remote logs from Cloud Logging.\n'
                    '[2022-01-01 00:00:10+00:00] {taskinstance.py:90} INFO - MSG1\n'
                    '[2022-01-01 00:00:10+00:00] {taskinstance.py:90} INFO - MSG2\n'
                    '[2022-01-01 00:00:10+00:00] {taskinstance.py:90} INFO - MSG1\n'
                    '[2022-01-01 00:00:10+00:00] {taskinstance.py:90} INFO - MSG2\n'
                    '[2022-01-01 00:00:10+00:00] {taskinstance.py:90} INFO - MSG1\n'
                    '[2022-01-01 00:00:10+00:00] {taskinstance.py:90} INFO - MSG2',
                ),
            )
        ] == logs
        assert [{'end_of_log': True}] == metadata

    @mock.patch('airflow.composer.composer_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.composer.composer_task_handler.LoggingServiceV2Client')
    def test_should_return_empty_logs_with_all_pages(self, mock_client, mock_get_creds_and_project_id):
        # A value for next_page_token can appear with empty log entries indicating
        # that Cloud Logging searching is not finished but so far there were no logs.
        # https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/list#response-body
        pages = [_create_list_log_entries_response_mock([], 'token') for i in range(2)]
        pages.append(_create_list_log_entries_response_mock([], None))
        mock_client.return_value.list_log_entries.return_value.pages = iter(pages)
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        logs, metadata = self.composerTaskHandler.read(self.ti, 1, {'download_logs': 'true'})

        log_filter = (
            'logName="projects/project_id/logs/airflow-worker"\n'
            'resource.type="cloud_composer_environment"\n'
            'resource.labels.project_id="project_id"\n'
            'resource.labels.environment_name="composer-env"\n'
            'resource.labels.location="loc"\n'
            'timestamp>="2022-01-01T00:00:00+00:00"\n'
            'timestamp<="2022-01-03T00:05:00+00:00"\n'
            'labels.task-id="task_for_testing_composer_log_handler"\n'
            'labels.workflow="dag_for_testing_composer_task_handler"\n'
            'labels.execution-date="2022-01-01T00:00:00+00:00"\n'
            'labels.try-number="1"\n'
            'labels.worker_id="default-hostname"'
        )

        assert [
            (
                (
                    'default-hostname',
                    '*** Reading remote logs from Cloud Logging.\n'
                    f'*** Logs not found for Cloud Logging filter:\n{log_filter}\n'
                    '*** The task might not have been executed, logs were deleted '
                    'as part of logs retention (default of 30 days),  or worker '
                    'executing it might have finished abnormally (e.g. was evicted).\n'
                    '*** Please, refer to '
                    'https://cloud.google.com/composer/docs/how-to/using/troubleshooting-dags#common_issues '
                    'for hints to learn what might be possible reasons for a missing log.',
                ),
            )
        ] == logs
        assert [{'end_of_log': True}] == metadata

    @mock.patch('airflow.composer.composer_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.composer.composer_task_handler.LoggingServiceV2Client')
    def test_should_return_empty_logs_with_paging(self, mock_client, mock_get_creds_and_project_id):
        pages = [_create_list_log_entries_response_mock([], 'token')]
        pages.append(_create_list_log_entries_response_mock([], None))
        mock_client.return_value.list_log_entries.return_value.pages = iter(pages)
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        page1_logs, metadata1 = self.composerTaskHandler.read(self.ti, 1)
        page2_logs, metadata2 = self.composerTaskHandler.read(self.ti, 1, metadata1[0])

        log_filter = (
            'logName="projects/project_id/logs/airflow-worker"\n'
            'resource.type="cloud_composer_environment"\n'
            'resource.labels.project_id="project_id"\n'
            'resource.labels.environment_name="composer-env"\n'
            'resource.labels.location="loc"\n'
            'timestamp>="2022-01-01T00:00:00+00:00"\n'
            'timestamp<="2022-01-03T00:05:00+00:00"\n'
            'labels.task-id="task_for_testing_composer_log_handler"\n'
            'labels.workflow="dag_for_testing_composer_task_handler"\n'
            'labels.execution-date="2022-01-01T00:00:00+00:00"\n'
            'labels.try-number="1"\n'
            'labels.worker_id="default-hostname"'
        )

        assert [
            (
                (
                    'default-hostname',
                    '*** Reading remote logs from Cloud Logging.\n',
                ),
            )
        ] == page1_logs
        assert [{'end_of_log': False, 'next_page_token': 'token'}] == metadata1
        assert [
            (
                (
                    'default-hostname',
                    f'*** Logs not found for Cloud Logging filter:\n{log_filter}\n'
                    '*** The task might not have been executed, logs were deleted '
                    'as part of logs retention (default of 30 days),  or worker '
                    'executing it might have finished abnormally (e.g. was evicted).\n'
                    '*** Please, refer to '
                    'https://cloud.google.com/composer/docs/how-to/using/troubleshooting-dags#common_issues '
                    'for hints to learn what might be possible reasons for a missing log.',
                ),
            )
        ] == page2_logs
        assert [{'end_of_log': True}] == metadata2

    @mock.patch('airflow.composer.composer_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.composer.composer_task_handler.LoggingServiceV2Client')
    def test_should_handle_permission_denied(self, mock_client, mock_get_creds_and_project_id):
        error = GoogleAPICallError("Nested permission denied message.")
        error.grpc_status_code = grpc.StatusCode.PERMISSION_DENIED
        mock_client.return_value.list_log_entries.side_effect = error
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        logs, metadata = self.composerTaskHandler.read(self.ti, 1)

        assert [
            (
                (
                    'default-hostname',
                    '*** Reading remote logs from Cloud Logging.\n'
                    f'{error.grpc_status_code}: The Service Account used by '
                    'the Composer environment is missing Composer Worker'
                    ' role.\n'
                    ' Please grant the role and retry.',
                ),
            )
        ] == logs
        assert [{'end_of_log': True}] == metadata

    @mock.patch('airflow.composer.composer_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.composer.composer_task_handler.LoggingServiceV2Client')
    def test_should_handle_retry_transient_error(self, mock_client, mock_get_creds_and_project_id):
        error = GoogleAPICallError("Nested service unavailable message.")
        error.grpc_status_code = grpc.StatusCode.UNAVAILABLE
        mock_client.return_value.list_log_entries.side_effect = error
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        logs, metadata = self.composerTaskHandler.read(self.ti, 1)

        assert [
            (
                (
                    'default-hostname',
                    '*** Reading remote logs from Cloud Logging.\n'
                    f'{error.grpc_status_code}: Transient server error '
                    'returned from Cloud Logging. Please try again.',
                ),
            )
        ] == logs
        assert [{'end_of_log': True}] == metadata

    @mock.patch('airflow.composer.composer_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.composer.composer_task_handler.LoggingServiceV2Client')
    def test_should_handle_quota_exceeded(self, mock_client, mock_get_creds_and_project_id):
        error = GoogleAPICallError("Nested quota exceeded message.")
        error.grpc_status_code = grpc.StatusCode.RESOURCE_EXHAUSTED
        mock_client.return_value.list_log_entries.side_effect = error
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        logs, metadata = self.composerTaskHandler.read(self.ti, 1)

        assert [
            (
                (
                    'default-hostname',
                    '*** Reading remote logs from Cloud Logging.\n'
                    f'{error.grpc_status_code}: {error.message}',
                ),
            )
        ] == logs
        assert [{'end_of_log': True}] == metadata

    @mock.patch('airflow.composer.composer_task_handler.get_credentials_and_project_id')
    @mock.patch('airflow.composer.composer_task_handler.LoggingServiceV2Client')
    def test_should_handle_other_error(self, mock_client, mock_get_creds_and_project_id):
        error = GoogleAPICallError("Nested error message.")
        error.grpc_status_code = 500
        mock_client.return_value.list_log_entries.side_effect = error
        mock_get_creds_and_project_id.return_value = ('creds', 'project_id')

        logs, metadata = self.composerTaskHandler.read(self.ti, 1)

        assert [
            (
                (
                    'default-hostname',
                    '*** Reading remote logs from Cloud Logging.\n'
                    f'Unexpected error occurred. {error.grpc_status_code}:'
                    f' {error.message}',
                ),
            )
        ] == logs
        assert [{'end_of_log': True}] == metadata

    def test_should_write_logs_to_stream(self):
        captured_output = io.StringIO(newline=None)
        composer_task_handler2 = ComposerTaskHandler(stream=captured_output)
        composer_task_handler2.setFormatter(TaskFormatter())
        composer_task_handler2.set_context(self.ti)
        composer_task_handler2.emit(
            logging.LogRecord(
                name='NAME',
                level='DEBUG',
                pathname=None,
                lineno=None,
                msg='MESSAGE',
                args=None,
                exc_info=None,
            )
        )
        composer_task_handler2.close()

        self.assertEqual(
            'MESSAGE@-@{"workflow": "dag_for_testing_composer_task_handler",'
            ' "task-id": "task_for_testing_composer_log_handler",'
            ' "execution-date": "2022-01-01T00:00:00+00:00",'
            ' "map-index": "-1",'
            ' "try-number": "1"}\n',
            captured_output.getvalue(),
        )

    def test_task_instance_to_labels(self):
        ti = mock.Mock(
            dag_id='test_dag_id',
            task_id='test_task_id',
            execution_date=timezone.datetime(2022, 1, 1),
            map_index=-1,
            try_number=1,
            hostname='test_hostname',
        )

        actual_labels = ComposerTaskHandler._task_instance_to_labels(ti)

        expected_labels = {
            'task-id': 'test_task_id',
            'workflow': 'test_dag_id',
            'execution-date': '2022-01-01T00:00:00+00:00',
            'try-number': '1',
            'worker_id': 'test_hostname',
        }
        self.assertEqual(actual_labels, expected_labels)

    def test_task_instance_to_labels_mapped_task(self):
        ti = mock.Mock(
            dag_id='test_dag_id',
            task_id='test_task_id',
            execution_date=timezone.datetime(2022, 1, 1),
            map_index=3,
            try_number=1,
            hostname='test_hostname',
        )

        actual_labels = ComposerTaskHandler._task_instance_to_labels(ti)

        expected_labels = {
            'task-id': 'test_task_id',
            'workflow': 'test_dag_id',
            'execution-date': '2022-01-01T00:00:00+00:00',
            'map-index': '3',
            'try-number': '1',
            'worker_id': 'test_hostname',
        }
        self.assertEqual(actual_labels, expected_labels)
