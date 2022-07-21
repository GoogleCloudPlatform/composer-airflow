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
import uuid
from typing import TYPE_CHECKING, Optional

from google.cloud.datacatalog.lineage.producer_client.v1.sync_lineage_client.sync_lineage_client import (
    SyncLineageClient,
)
from google.cloud.datacatalog.lineage_v1 import CreateLineageEventsBundleRequest

from airflow.composer.data_lineage.adapter import ComposerDataLineageAdapter
from airflow.composer.data_lineage.utils import LOCATION_PATH
from airflow.lineage.backend import LineageBackend

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator

log = logging.getLogger(__name__)


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

        lineage_events_bundle = _adapter.get_lineage_events_bundle_on_task_completed(
            context["ti"], inlets, outlets
        )
        log.info("Lineage events bundle: %s", lineage_events_bundle)

        request_id = uuid.uuid4().hex
        request = CreateLineageEventsBundleRequest(
            parent=LOCATION_PATH,
            lineage_events_bundle=lineage_events_bundle,
            request_id=request_id,
        )

        # TODO: log lineage events bundle
        # TODO: retries, pass retry parameter
        _client.create_events_bundle(request)
        # TODO: log response status, ...
        log.info("Lineage metadata sent successfully")
