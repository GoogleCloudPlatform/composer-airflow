#
# Copyright 2020 Google LLC
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
"""Composer Data Lineage backend implementation."""
import logging
from typing import TYPE_CHECKING, Optional

from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.api_core.retry import Retry
from google.cloud.datacatalog.lineage.producer_client.v1.sync_lineage_client.sync_lineage_client import (
    SyncLineageClient,
)

from airflow.composer.data_lineage.adapter import ComposerDataLineageAdapter
from airflow.composer.task_formatter import _EXTRA_WORKFLOW_INFO_RECORD_KEY
from airflow.lineage.backend import LineageBackend

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator

log = logging.getLogger(__name__)
log = logging.LoggerAdapter(log, {_EXTRA_WORKFLOW_INFO_RECORD_KEY: {"log-type": "data_lineage"}})


class ComposerDataLineageBackend(LineageBackend):
    """Airflow lineage backend to send lineage metadata to Data Lineage API."""

    def send_lineage(
        self,
        operator: "BaseOperator",
        inlets: Optional[list] = None,
        outlets: Optional[list] = None,
        context: Optional[dict] = None,
    ) -> None:
        """Sends lineage metadata to Data Lineage API.

        For arguments description see base class.
        """
        # We construct client and adapter here but not in constructor of backend because initialization of
        # lineage backend happens on BaseOperator module import.
        _client = SyncLineageClient()
        _adapter = ComposerDataLineageAdapter()
        ti = context["ti"]

        lineage_events_bundle = _adapter.get_lineage_events_bundle_on_task_completed(ti, inlets, outlets)

        lineage_events_bundle_message = "\n".join(
            [
                f"dag_id={ti.task.dag.dag_id}, task_id={ti.task.task_id}, run_id={ti.run_id}, "
                f"try_number={ti.try_number}, operator={type(ti.task).__name__}",
                f"Process: name={lineage_events_bundle['process'].name}",
                f"Run: name={lineage_events_bundle['run'].name}, state={lineage_events_bundle['run'].state}",
            ]
            + [
                f"LineageEvent: links={le.links}, start_time={le.start_time}, end_time={le.end_time}"
                for le in lineage_events_bundle["lineage_events"]
            ]
        )

        try:
            _client.create_events_bundle(
                process=lineage_events_bundle["process"],
                run=lineage_events_bundle["run"],
                events=lineage_events_bundle["lineage_events"],
                retry=Retry(deadline=5),
            )
        except (GoogleAPICallError, RetryError):
            log.exception("Failed to send data lineage %s", lineage_events_bundle_message)
        else:
            log.info("Data lineage sent %s", lineage_events_bundle_message)
