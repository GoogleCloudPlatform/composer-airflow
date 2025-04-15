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
import os
from functools import cached_property
from typing import TYPE_CHECKING

import grpc
from google.api_core.exceptions import GoogleAPICallError
from google.api_core.gapic_v1.client_info import ClientInfo
from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
from google.cloud.logging_v2.types import ListLogEntriesRequest
from google.logging.type import log_severity_pb2
from sqlalchemy import select

from airflow import version
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.utils.log.log_reader import StructuredLogMessage
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.timezone import utcnow

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models import TaskInstance
    from airflow.utils.log.log_reader import LogMessages, LogMetadata

PROJECT = os.environ["GCP_PROJECT"]
ENVIRONMENT_LOCATION = os.environ["COMPOSER_LOCATION"]
ENVIRONMENT_NAME = os.environ["COMPOSER_ENVIRONMENT"]
CLOUD_LOGGING_LOGS_PAGE_SIZE = 1000  # maximum that Cloud Logging allows.
TI_START_DATE_FILTER_OFFSET = datetime.timedelta(minutes=5)


class TaskLogReaderHandler(LoggingMixin):
    """Handler to read task logs."""

    def read(
        self, task_instance: TaskInstance, try_number: int | None, metadata: dict | None
    ) -> tuple[LogMessages, LogMetadata]:
        """Read logs of the given task instance from Cloud Logging."""
        if try_number is None:
            try_number = task_instance.try_number

        ti_start_date = self._get_ti_start_date(task_instance, try_number)
        if ti_start_date is None:
            return [
                StructuredLogMessage(
                    timestamp=utcnow(),
                    level="debug",
                    event=(
                        "Looks like the task didn't start yet (`start_date` of the task instance is empty). "
                        "Please retry later."
                    ),
                ),
            ], {"end_of_log": True}

        logs_filter = self._get_logs_filter(task_instance, try_number, ti_start_date)
        log_messages, log_metadata = self._read_single_page(logs_filter=logs_filter)

        # If it is the last page and there are no logs, return message with information on possible reasons
        # and how to troubleshoot.
        # TODO: what if there were messages on previous pages?
        if not log_messages and not log_metadata.get("next_page_token"):
            log_messages = [
                StructuredLogMessage(
                    timestamp=utcnow(),
                    level="error",
                    event="\n".join(
                        [
                            "Logs not found. The possible reasons are:",
                            "*** the task is not yet executed",
                            "*** worker executing it might have finished abnormally (e.g. was evicted)",
                            "*** the task is executed, but logs are not yet propagated",
                            (
                                "*** the task is executed, but logs were deleted as part of logs retention "
                                "(default of 30 days)"
                            ),
                            (
                                "Please, refer to "
                                "https://cloud.google.com/composer/docs/how-to/using/troubleshooting-dags#common_issues "
                                "for details on troubleshooting."
                            ),
                        ]
                    ),
                ),
            ]

        # Prepend log messages with message containing filter used to query Cloud Logging.
        logs_filter_formatted = logs_filter.replace("\n", " ")
        log_messages = [
            StructuredLogMessage(
                timestamp=utcnow(),
                level="debug",
                event=f"Reading logs from Cloud Logging using filter:\n{logs_filter_formatted}",
            ),
        ] + log_messages
        # TODO: support pagination on the backend, once we rebase on the version of Airflow where pagination
        # is supported on the UI side.
        log_metadata["end_of_log"] = True

        return log_messages, log_metadata

    @cached_property
    def _client(self) -> LoggingServiceV2Client:
        client = LoggingServiceV2Client(
            client_info=ClientInfo(client_library_version=f"airflow_v{version.version}"),
        )
        return client

    @provide_session
    def _get_ti_start_date(
        self,
        task_instance: TaskInstance,
        try_number: int,
        session: Session = NEW_SESSION,
    ):
        """Return task instance start date that will be used for Cloud Logging filter."""
        if task_instance.try_number == try_number:
            return task_instance.start_date

        query = select(TaskInstanceHistory).where(
            TaskInstanceHistory.task_id == task_instance.task_id,
            TaskInstanceHistory.dag_id == task_instance.dag_id,
            TaskInstanceHistory.run_id == task_instance.run_id,
            TaskInstanceHistory.map_index == task_instance.map_index,
            TaskInstanceHistory.try_number == try_number,
        )
        tih = session.scalar(query)
        if tih:
            return tih.start_date

        return None

    def _get_logs_filter(
        self,
        task_instance: TaskInstance,
        try_number: int,
        ti_start_date: datetime.datetime,
    ) -> str:
        """Return log filter that should be used to query Cloud Logging."""
        filters = []

        # Airflow worker logs of Composer Environments in the current project.
        filters.extend(
            [
                f'logName=("projects/{PROJECT}/logs/airflow-worker" OR "projects/{PROJECT}/logs/airflow-k8s-worker")',
                'resource.type="cloud_composer_environment"',
            ]
        )

        # + only logs of the current Composer Environment.
        filters.extend(
            [
                f'resource.labels.project_id="{PROJECT}"',
                f'resource.labels.location="{ENVIRONMENT_LOCATION}"',
                f'resource.labels.environment_name="{ENVIRONMENT_NAME}"',
            ]
        )

        # + only logs of the given task instance.
        filters.extend(
            [
                f'labels.workflow="{task_instance.dag_id}"',
                f'labels.task-id="{task_instance.task_id}"',
                f'labels.run-id="{task_instance.run_id}"',
            ]
        )
        if task_instance.map_index != -1:
            # If we add "map-index" label always to filter, this will not work for logs in Cloud Logging that
            # were emitted in Composer image versions with Airflow prior to 2.3.3, because they do not
            # have "map-index" label.
            # For the logs from not mapped tasks that have "map-index" label (emitted with Airflow 2.3.3+)
            # there is no difference to set this filter for or not, as for all of them "map-index" label has
            # "-1" as a value equal to ti.map_index.
            filters.append(
                f'labels.map-index="{task_instance.map_index}"',
            )
        filters.append(f'labels.try-number="{try_number}"')

        # + filter by timestamp to optimize query performance.
        ti_start_date_with_offset = ti_start_date - TI_START_DATE_FILTER_OFFSET
        filters.append(f'timestamp>="{str(ti_start_date_with_offset.isoformat())}"')

        return "\n".join(filters)

    def _read_single_page(self, logs_filter: str) -> tuple[LogMessages, LogMetadata]:
        """Read one page of Cloud Logging logs."""
        request = ListLogEntriesRequest(
            resource_names=[f"projects/{PROJECT}"],
            filter=logs_filter,
            page_token=None,
            order_by="timestamp asc",
            page_size=CLOUD_LOGGING_LOGS_PAGE_SIZE,
        )

        log_messages = []
        log_metadata = {}
        try:
            response = self._client.list_log_entries(request=request)

            page = next(response.pages)
            for entry in page.entries:
                log_messages.append(
                    StructuredLogMessage(
                        timestamp=entry.timestamp,
                        level=log_severity_pb2.LogSeverity.Name(entry.severity),
                        event=entry.text_payload,
                    )
                )
            log_metadata["next_page_token"] = page.next_page_token
        except GoogleAPICallError as e:
            if e.grpc_status_code == grpc.StatusCode.PERMISSION_DENIED:
                error = (
                    f"{e.grpc_status_code}: The Service Account used by the Composer environment is missing "
                    "Composer Worker role.\n  Please grant the role and retry."
                )
            elif e.grpc_status_code == grpc.StatusCode.RESOURCE_EXHAUSTED:
                error = f"{e.grpc_status_code}: {e.message}"
            elif e.grpc_status_code == grpc.StatusCode.UNAVAILABLE:
                error = (
                    f"{e.grpc_status_code}: Transient server error returned from Cloud Logging. "
                    "Please try again."
                )
            else:
                error = f"Unexpected error occurred. {e.grpc_status_code}: {e.message}"

            self.log.error(e)
            return [StructuredLogMessage(timestamp=utcnow(), level="error", event=error)], {}

        return log_messages, log_metadata
