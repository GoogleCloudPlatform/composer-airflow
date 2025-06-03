#
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import datetime
import random
import string
from unittest import mock

import pytest
import time_machine
from google.api_core.exceptions import (
    DeadlineExceeded,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
)
from google.cloud.logging_v2.types import ListLogEntriesRequest
from google.logging.type import log_severity_pb2

from airflow.models import DagRun, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.utils import timezone
from airflow.utils.log.log_reader import StructuredLogMessage
from airflow.utils.session import provide_session
from airflow.version import version


class TestTaskLogReaderHandler:
    def create_handler(self):
        with mock.patch.dict(
            "os.environ",
            {
                "GCP_PROJECT": "test-project",
                "COMPOSER_LOCATION": "test-location",
                "COMPOSER_ENVIRONMENT": "test-environment",
            },
        ):
            from airflow.composer.patches.logging.task_log_reader_handler import TaskLogReaderHandler

        return TaskLogReaderHandler()

    def setup_method(self):
        self.handler = self.create_handler()

    @pytest.mark.parametrize(
        "task_instance_mock, try_number, expected_get_logs_filter_try_number_param",
        [
            (
                mock.Mock(),
                2,
                2,
            ),
            (
                mock.Mock(try_number=5),
                None,
                5,
            ),
        ],
    )
    @time_machine.travel("2013-01-03", tick=False)
    def test_read(self, task_instance_mock, try_number, expected_get_logs_filter_try_number_param):
        handler = self.create_handler()
        handler._get_ti_start_date = mock.Mock(return_value="start-date")
        handler._get_logs_filter = mock.Mock(return_value="logs-filter")
        handler._read_single_page = mock.Mock(
            return_value=([StructuredLogMessage(timestamp=datetime.datetime(2010, 1, 1), event="event")], {})
        )

        actual_log_messages, actual_log_metadata = handler.read(
            task_instance=task_instance_mock, try_number=try_number, metadata=None
        )

        handler._get_ti_start_date.assert_called_once_with(
            task_instance_mock, expected_get_logs_filter_try_number_param
        )
        handler._get_logs_filter.assert_called_once_with(
            task_instance_mock, expected_get_logs_filter_try_number_param, "start-date"
        )
        handler._read_single_page.assert_called_once_with(logs_filter="logs-filter")
        assert actual_log_messages == [
            StructuredLogMessage(
                timestamp=datetime.datetime(2013, 1, 3, tzinfo=timezone.utc),
                level="debug",
                event="Reading logs from Cloud Logging using filter:\nlogs-filter",
            ),
            StructuredLogMessage(timestamp=datetime.datetime(2010, 1, 1), event="event"),
        ]
        assert actual_log_metadata == {"end_of_log": True}

    @time_machine.travel("2013-01-03", tick=False)
    def test_read_empty_ti_start_date(self):
        handler = self.create_handler()
        handler._get_ti_start_date = mock.Mock(return_value=None)

        actual_log_messages, actual_log_metadata = handler.read(
            task_instance=mock.Mock(), try_number=5, metadata=None
        )

        assert actual_log_messages == [
            StructuredLogMessage(
                timestamp=datetime.datetime(2013, 1, 3, tzinfo=timezone.utc),
                level="debug",
                event=(
                    "Looks like the task didn't start yet (`start_date` of the task instance is empty). "
                    "Please retry later."
                ),
            ),
        ]
        assert actual_log_metadata == {"end_of_log": True}

    @time_machine.travel("2013-01-03", tick=False)
    def test_read_last_page_no_logs(self):
        handler = self.create_handler()
        handler._get_ti_start_date = mock.Mock(return_value="start-date")
        handler._get_logs_filter = mock.Mock(return_value="logs-filter")
        handler._read_single_page = mock.Mock(return_value=([], {"next_page_token": None}))

        actual_log_messages, actual_log_metadata = handler.read(
            task_instance=mock.Mock(), try_number=3, metadata=None
        )

        assert actual_log_messages == [
            StructuredLogMessage(
                timestamp=datetime.datetime(2013, 1, 3, tzinfo=timezone.utc),
                level="debug",
                event="Reading logs from Cloud Logging using filter:\nlogs-filter",
            ),
            StructuredLogMessage(
                timestamp=datetime.datetime(2013, 1, 3, tzinfo=timezone.utc),
                level="error",
                event=(
                    "Logs not found. The possible reasons are:\n"
                    "*** the task is not yet executed\n"
                    "*** worker executing it might have finished abnormally (e.g. was evicted)\n"
                    "*** the task is executed, but logs are not yet propagated\n"
                    "*** the task is executed, but logs were deleted as part of logs retention "
                    "(default of 30 days)\n"
                    "Please, refer to "
                    "https://cloud.google.com/composer/docs/how-to/using/troubleshooting-dags#common_issues "
                    "for details on troubleshooting."
                ),
            ),
        ]
        assert actual_log_metadata == {"end_of_log": True, "next_page_token": None}

    def test_get_ti_start_date_from_task_instance(self):
        actual_start_date = self.handler._get_ti_start_date(
            mock.MagicMock(try_number=34, start_date=datetime.datetime(2011, 4, 7)),
            34,
        )

        assert actual_start_date == datetime.datetime(2011, 4, 7)

    @provide_session
    def test_get_ti_start_date_from_task_instance_history(self, session):
        dag_id = "dag-id"
        task_id = "task-id"
        run_id = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        map_index = 2
        try_number = 5

        dr = DagRun(
            dag_id=dag_id,
            run_id=run_id,
            run_type="manual",
        )
        ti = TaskInstance(
            task=BaseOperator(task_id=task_id),
            run_id=run_id,
            map_index=map_index,
        )
        ti.dag_id = dag_id
        ti.try_number = try_number
        ti.start_date = datetime.datetime(2010, 1, 8, tzinfo=timezone.utc)
        session.add(dr)
        session.add(ti)
        session.commit()

        tih = TaskInstanceHistory(ti)
        session.add(tih)
        session.commit()

        actual_start_date = self.handler._get_ti_start_date(
            mock.MagicMock(
                task_id=task_id,
                dag_id=dag_id,
                run_id=run_id,
                map_index=map_index,
                try_number=34,  # different from "try_number" variable.
                start_date=datetime.datetime(2011, 4, 7),
            ),
            try_number,
        )

        assert actual_start_date == datetime.datetime(2010, 1, 8, tzinfo=timezone.utc)

    def test_get_ti_start_date_none(self):
        actual_start_date = self.handler._get_ti_start_date(
            mock.MagicMock(
                task_id="non-exist",
                dag_id="non-exist",
                run_id="non-exist",
                map_index=345,
                try_number=1,
                start_date=datetime.datetime(2011, 4, 7),
            ),
            2,
        )

        assert actual_start_date is None

    @pytest.mark.parametrize(
        "task_instance, try_number, expected_result",
        [
            (
                mock.Mock(
                    dag_id="dag-id",
                    task_id="task-id",
                    run_id="run-id",
                    map_index=3,
                ),
                34,
                "\n".join(
                    [
                        'logName=("projects/test-project/logs/airflow-worker" OR "projects/test-project/logs/airflow-k8s-worker")',
                        'resource.type="cloud_composer_environment"',
                        'resource.labels.project_id="test-project"',
                        'resource.labels.location="test-location"',
                        'resource.labels.environment_name="test-environment"',
                        'labels.workflow="dag-id"',
                        'labels.task-id="task-id"',
                        'labels.run-id="run-id"',
                        'labels.map-index="3"',
                        'labels.try-number="34"',
                        'timestamp>="2010-03-05T00:57:03"',
                    ]
                ),
            ),
            (
                mock.Mock(
                    dag_id="dag-id",
                    task_id="task-id",
                    run_id="run-id",
                    map_index=-1,
                ),
                34,
                "\n".join(
                    [
                        'logName=("projects/test-project/logs/airflow-worker" OR "projects/test-project/logs/airflow-k8s-worker")',
                        'resource.type="cloud_composer_environment"',
                        'resource.labels.project_id="test-project"',
                        'resource.labels.location="test-location"',
                        'resource.labels.environment_name="test-environment"',
                        'labels.workflow="dag-id"',
                        'labels.task-id="task-id"',
                        'labels.run-id="run-id"',
                        'labels.try-number="34"',
                        'timestamp>="2010-03-05T00:57:03"',
                    ]
                ),
            ),
        ],
    )
    def test_get_logs_filter(self, task_instance, try_number, expected_result):
        actual_logs_filter = self.handler._get_logs_filter(
            task_instance=task_instance,
            try_number=try_number,
            ti_start_date=datetime.datetime(2010, 3, 5, 1, 2, 3),
        )

        assert actual_logs_filter == expected_result

    @mock.patch(
        "airflow.composer.patches.logging.task_log_reader_handler.LoggingServiceV2Client", autospec=True
    )
    def test_read_single_page(self, logging_client_mock):
        client_mock = mock.Mock(
            list_log_entries=mock.Mock(
                return_value=mock.Mock(
                    pages=iter(
                        [
                            mock.Mock(
                                entries=[
                                    mock.Mock(
                                        timestamp=datetime.datetime(2010, 1, 1),
                                        severity=log_severity_pb2.LogSeverity.INFO,
                                        text_payload="text-payload",
                                    )
                                ],
                                next_page_token="page token 123",
                            ),
                        ]
                    )
                )
            )
        )
        logging_client_mock.return_value = client_mock

        actual_log_messages, actual_log_metadata = self.handler._read_single_page("logs-filter")

        logging_client_mock.assert_called_once()
        assert (
            logging_client_mock.call_args_list[0][1]["client_info"].client_library_version
            == "airflow_v" + version
        )
        client_mock.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/test-project"],
                filter="logs-filter",
                page_token=None,
                order_by="timestamp asc",
                page_size=1000,
            )
        )
        assert actual_log_messages == [
            StructuredLogMessage(timestamp=datetime.datetime(2010, 1, 1), level="INFO", event="text-payload")
        ]
        assert actual_log_metadata == {"next_page_token": "page token 123"}

    @pytest.mark.parametrize(
        "error, expected_event",
        [
            (
                PermissionDenied("error"),
                (
                    "StatusCode.PERMISSION_DENIED: The Service Account used by the Composer environment is "
                    "missing Composer Worker role.\n  Please grant the role and retry."
                ),
            ),
            (
                ResourceExhausted("error"),
                ("StatusCode.RESOURCE_EXHAUSTED: error"),
            ),
            (
                ServiceUnavailable("error"),
                (
                    "StatusCode.UNAVAILABLE: Transient server error returned from Cloud Logging. Please try again."
                ),
            ),
            (
                DeadlineExceeded("error"),
                ("Unexpected error occurred. StatusCode.DEADLINE_EXCEEDED: error"),
            ),
        ],
    )
    @mock.patch(
        "airflow.composer.patches.logging.task_log_reader_handler.LoggingServiceV2Client", autospec=True
    )
    @time_machine.travel("2016-01-01", tick=False)
    def test_read_single_page_errors(self, logging_client_mock, error, expected_event):
        client_mock = mock.Mock(list_log_entries=mock.Mock(side_effect=error))
        logging_client_mock.return_value = client_mock

        actual_log_messages, actual_log_metadata = self.handler._read_single_page("logs-filter")

        assert actual_log_messages == [
            StructuredLogMessage(
                timestamp=datetime.datetime(2016, 1, 1, tzinfo=timezone.utc),
                level="error",
                event=expected_event,
            ),
        ]
        assert actual_log_metadata == {}
