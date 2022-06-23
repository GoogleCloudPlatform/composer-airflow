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
"""Handler that integrates with Cloud Logging."""
from typing import Collection, Dict, List, Optional, Tuple

from airflow.utils.log.logging_mixin import LoggingMixin

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
from google.api_core.gapic_v1.client_info import ClientInfo
from google.auth.credentials import Credentials
from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
from google.cloud.logging_v2.types import ListLogEntriesRequest, ListLogEntriesResponse, LogEntry
from google.logging.type import log_severity_pb2

from airflow import version
from airflow.models import TaskInstance
from airflow.providers.google.cloud.utils.credentials_provider import get_credentials_and_project_id
from airflow.utils.log.file_task_handler import StreamTaskHandler

DEFAULT_LOGGER_NAME = "airflow-worker"

_DEFAULT_SCOPES = frozenset(
    ["https://www.googleapis.com/auth/logging.read", "https://www.googleapis.com/auth/logging.write"]
)


class ComposerTaskHandler(StreamTaskHandler, LoggingMixin):
    """Handler that directly makes Cloud logging API calls.

    This is a Python standard ``logging`` handler using that can be used to
    route Python standard logging messages directly to the provided stream.

    It can also be used to read logs for executing tasks. To do this, you should
    set as a handler with the name "tasks". In this case, it will also be used
    to read the log for display in Web UI.

    :param gcp_key_path: Path to Google Cloud Credential JSON file.
        If omitted, authorization based on `the Application Default Credentials
        <https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__ will
        be used.
    :type gcp_key_path: str
    :param scopes: OAuth scopes for the credentials,
    :type scopes: Sequence[str]
    :param labels: (Optional) Mapping of labels for the entry.
    :type labels: dict
    """

    LABEL_TASK_ID = "task-id"
    LABEL_DAG_ID = "workflow"
    LABEL_EXECUTION_DATE = "execution-date"
    LABEL_TRY_NUMBER = "try-number"
    LOG_NAME = 'Google Composer Task Logger'

    def __init__(
        self,
        gcp_key_path: Optional[str] = None,
        scopes: Optional[Collection[str]] = _DEFAULT_SCOPES,
        labels: Optional[Dict[str, str]] = None,
        stream=None,
    ):
        super().__init__(stream=stream)
        self.gcp_key_path: Optional[str] = gcp_key_path
        self.scopes: Optional[Collection[str]] = scopes
        self.labels: Optional[Dict[str, str]] = labels
        self.task_instance_labels: Optional[Dict[str, str]] = {}
        self.task_instance_hostname = 'default-hostname'

    @cached_property
    def _credentials_and_project(self) -> Tuple[Credentials, str]:
        credentials, project = get_credentials_and_project_id(
            key_path=self.gcp_key_path, scopes=self.scopes, disable_logging=True
        )
        return credentials, project

    @property
    def _logging_service_client(self) -> LoggingServiceV2Client:
        """The Cloud logging service v2 client."""
        credentials, _ = self._credentials_and_project
        client = LoggingServiceV2Client(
            credentials=credentials,
            client_info=ClientInfo(client_library_version='airflow_v' + version.version),
        )
        return client

    def read(
        self, task_instance: TaskInstance, try_number: Optional[int] = None, metadata: Optional[Dict] = None
    ) -> Tuple[List[Tuple[Tuple[str, str]]], List[Dict[str, str]]]:
        """
        Read logs of given task instance from Cloud logging.

        :param task_instance: task instance object
        :type task_instance: :class:`airflow.models.TaskInstance`
        :param try_number: task instance try_number to read logs from. If None
           it returns all logs
        :type try_number: Optional[int]
        :param metadata: log metadata. It is used for steaming log reading and auto-tailing.
        :type metadata: Dict
        :return: a tuple of (
            list of (one element tuple with two element tuple - hostname and logs)
            and list of metadata)
        :rtype: Tuple[List[Tuple[Tuple[str, str]]], List[Dict[str, str]]]
        """
        self.log.warning("Reading logs with ComposerTaskHandler for " + str(task_instance))
        if try_number is not None and try_number < 1:
            logs = f"Error fetching the logs. Try number {try_number} is invalid."
            return [((self.task_instance_hostname, logs),)], [{"end_of_log": "true"}]

        if not metadata:
            metadata = {}

        log_filter = self._prepare_filter(task_instance, try_number)
        self.log.warning("Log filter is " + log_filter)
        next_page_token = metadata.get("next_page_token", None)
        all_pages = 'download_logs' in metadata and metadata['download_logs']

        messages, end_of_log, next_page_token = self._read_logs(log_filter, next_page_token, all_pages)

        new_metadata = {"end_of_log": end_of_log}

        if next_page_token:
            new_metadata['next_page_token'] = next_page_token

        return [((self.task_instance_hostname, messages),)], [new_metadata]

    def _prepare_filter(self, task_instance, try_number) -> str:
        """
        Prepares the filter that chooses which log entries to fetch.

        More information:
        https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/list#body.request_body.FIELDS.filter
        https://cloud.google.com/logging/docs/view/advanced-queries

        :param task_instance: task instance object
        :type task_instance: :class:`airflow.models.TaskInstance`
        :param try_number: task instance try_number to read logs from. If None
           it returns all logs
        :type try_number: Optional[int]
        :return: logs filter
        """

        def escape_label_key(key: str) -> str:
            return f'"{key}"' if "." in key else key

        def escape_label_value(value: str) -> str:
            escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped_value}"'

        _, project = self._credentials_and_project
        log_filters = [
            'resource.type="cloud_composer_environment"',
            f'logName="projects/{project}/logs/airflow-worker"',
            f'timestamp>="{str(task_instance.start_date.isoformat())}"',
        ]
        if task_instance.end_date is not None:
            log_filters.append(f'timestamp<="{str(task_instance.end_date.isoformat())}"')

        ti_labels = self._task_instance_to_labels(task_instance)
        if try_number is not None:
            ti_labels[self.LABEL_TRY_NUMBER] = str(try_number)
        else:
            del ti_labels[self.LABEL_TRY_NUMBER]

        for key, value in ti_labels.items():
            log_filters.append(f'labels.{escape_label_key(key)}={escape_label_value(value)}')
        return "\n".join(log_filters)

    def _read_logs(
        self, log_filter: str, next_page_token: Optional[str], all_pages: bool
    ) -> Tuple[str, bool, Optional[str]]:
        """
        Sends requests to the Cloud Logging service and downloads logs.

        :param log_filter: Filter specifying the logs to be downloaded.
        :type log_filter: str
        :param next_page_token: The token of the page from which the log download will start.
            If None is passed, it will start from the first page.
        :param all_pages: If True is passed, all subpages will be downloaded. Otherwise, only the first
            page will be downloaded
        :return: A token that contains the following items:
            * string with logs
            * Boolean value describing whether there are more logs,
            * token of the next page
        :rtype: Tuple[str, bool, str]
        """
        messages = []
        new_messages, next_page_token = self._read_single_logs_page(
            log_filter=log_filter,
            page_token=next_page_token,
        )
        messages.append(new_messages)
        if all_pages:
            while next_page_token:
                new_messages, next_page_token = self._read_single_logs_page(
                    log_filter=log_filter, page_token=next_page_token
                )
                messages.append(new_messages)
                if not messages:
                    break

            end_of_log = True
            next_page_token = None
        else:
            end_of_log = not bool(next_page_token)
        return "\n".join(messages), end_of_log, next_page_token

    def _read_single_logs_page(self, log_filter: str, page_token: Optional[str] = None) -> Tuple[str, str]:
        """
        Sends requests to the Cloud Logging service and downloads single page with logs.

        :param log_filter: Filter specifying the logs to be downloaded.
        :type log_filter: str
        :param page_token: The token of the page to be downloaded. If None is passed, the first page will be
            downloaded.
        :type page_token: str
        :return: Downloaded logs and next page token
        :rtype: Tuple[str, str]
        """
        _, project = self._credentials_and_project
        request = ListLogEntriesRequest(
            resource_names=[f'projects/{project}'],
            filter=log_filter,
            page_token=page_token,
            order_by='timestamp asc',
            page_size=1000,
        )
        response = self._logging_service_client.list_log_entries(request=request)
        page: ListLogEntriesResponse = next(response.pages)
        messages = []
        for entry in page.entries:
            messages.append(self._format_entry(entry))
        return "\n".join(messages), page.next_page_token

    @staticmethod
    def _format_entry(entry: LogEntry) -> str:
        return f'[{entry.timestamp}] {{{entry.labels["process"]}}} {log_severity_pb2.LogSeverity.Name(entry.severity)} - {entry.text_payload}'

    @classmethod
    def _task_instance_to_labels(cls, ti: TaskInstance) -> Dict[str, str]:
        return {
            cls.LABEL_TASK_ID: ti.task_id,
            cls.LABEL_DAG_ID: ti.dag_id,
            cls.LABEL_EXECUTION_DATE: str(ti.execution_date.isoformat()),
            cls.LABEL_TRY_NUMBER: str(ti.try_number),
        }

    @property
    def log_name(self):
        """Return log name."""
        return self.LOG_NAME
