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

import json
import logging
import os

from google.api_core.retry import Retry
from google.cloud.datacatalog.lineage.producer_client.v1 import SyncLineageClient
from google.cloud.datacatalog_lineage_v1 import ProcessOpenLineageRunEventRequest
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from typing import TYPE_CHECKING

from airflow.composer.openlineage.facets import (
    ComposerJobFacet,
    ComposerRunFacet,
    GcpLineageJobFacet,
    GcpOrigin,
)
from airflow.composer.openlineage.utils import sanitize_display_name, get_redacted_event
from airflow.composer.task_formatter import _EXTRA_WORKFLOW_INFO_RECORD_KEY
from airflow.version import version as airflow_version

if TYPE_CHECKING:
    from openlineage.client.client import Event

# Side channel header according to Cloud Data Lineage documentation.
SIDECHANNEL_HEADER = "x-goog-ext-512598505-bin"
SIDECHANNEL_VALUES = {
    "DAG": b"\n\x08COMPOSER\x12\x03DAG",
    "TASK": b"\n\x08COMPOSER\x12\x04TASK",
}
UNKNOWN_TYPE_SIDECHANNEL_VALUE = b"\n\x08COMPOSER\x12\x07UNKNOWN"

COMPOSER_ENVIRONMENT_NAME = os.environ.get("COMPOSER_ENVIRONMENT")
LOCATION_PATH = f"projects/{os.environ.get('GCP_PROJECT')}/locations/{os.environ.get('COMPOSER_LOCATION')}"
COMPOSER_VERSION = os.environ.get("COMPOSER_VERSION")

log: logging.Logger | logging.LoggerAdapter = logging.getLogger(__name__)
log = logging.LoggerAdapter(log, {_EXTRA_WORKFLOW_INFO_RECORD_KEY: {"log-type": "data_lineage"}})


class ComposerTransportConfig(Config): ...


class ComposerTransport(Transport):
    """Transport layer that sends events to the Cloud Data Lineage OL Endpoint.

    Also injects Composer and GCP specific facets to each event.
    """
    kind = "ComposerTransport"
    config_class = ComposerTransportConfig

    def __init__(self, config: ComposerTransportConfig) -> None:
        self.client = SyncLineageClient()

    def emit(self, event: Event) -> None:
        try:
            log.info("Sending the event to the Data Lineage OpenLineage API.")
            self._patch_event(event)
            event_dict = json.loads(Serde.to_json(event))
            log.info(get_redacted_event(event_dict))

            # We use the openlineage SerDe module to send the event as a dictionary
            request = ProcessOpenLineageRunEventRequest(
                {"parent": LOCATION_PATH, "open_lineage": event_dict}
            )
            job_type = event.job.facets["jobType"].jobType
            sidechannel_value = SIDECHANNEL_VALUES.get(job_type, UNKNOWN_TYPE_SIDECHANNEL_VALUE)
            response = self.client.process_open_lineage_run_event(
                request,
                metadata=[
                    (SIDECHANNEL_HEADER, sidechannel_value),
                ],
                retry=Retry(deadline=5),
            )
            response_message = f"response: process={response.process}, run={response.run}"
            if response.lineage_events:
                response_message = "\n".join([response_message, f"lineage_events={response.lineage_events}"])
            log.info(response_message)
        except Exception:
            log.exception("Failed to send the event to DataLineage API.")

    def _patch_event(self, event):
        """Add Composer and GCP specific facets to the event."""
        job_type = event.job.facets["jobType"].jobType
        if job_type == "DAG":
            composer_job_facet = ComposerJobFacet(
                environmentName=COMPOSER_ENVIRONMENT_NAME,
                composerVersion=COMPOSER_VERSION,
                airflowVersion=airflow_version,
                dagId=event.job.name,
                taskId=None,
                operator=None,
            )
            run_id = (
                event.run.facets["airflowDagRun"].dagRun["run_id"]
                if event.run.facets.get("airflowDagRun")
                else None
            )
            composer_run_facet = ComposerRunFacet(dagRunId=run_id)
        elif job_type == "TASK":
            composer_job_facet = ComposerJobFacet(
                environmentName=COMPOSER_ENVIRONMENT_NAME,
                composerVersion=COMPOSER_VERSION,
                airflowVersion=airflow_version,
                dagId=event.run.facets["airflow"].dag["dag_id"],
                taskId=event.run.facets["airflow"].task["task_id"],
                operator=event.run.facets["airflow"].task["operator_class"],
            )
            composer_run_facet = ComposerRunFacet(dagRunId=event.run.facets["airflow"].dagRun["run_id"])
        else:
            log.warning("Unrecognized OpenLineage JobType %s", job_type)
            return

        gcp_lineage_facet = GcpLineageJobFacet(
            displayName=sanitize_display_name(
                f"Composer Airflow {job_type.capitalize()} {COMPOSER_ENVIRONMENT_NAME}.{event.job.name}"
            ),
            origin=GcpOrigin(
                sourceType="COMPOSER",
                name=os.path.join(
                    LOCATION_PATH, f"environments/{COMPOSER_ENVIRONMENT_NAME}"
                ),
            ),
        )

        event.job.facets["gcp_composer_job"] = composer_job_facet
        event.run.facets["gcp_composer_run"] = composer_run_facet
        event.job.facets["gcp_lineage"] = gcp_lineage_facet
