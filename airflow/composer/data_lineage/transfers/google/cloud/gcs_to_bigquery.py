#
# Copyright 2021 Google LLC
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

import logging

from airflow import AirflowException
from airflow.composer.data_lineage.entities import BigQueryTable, GCSEntity
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

log = logging.getLogger(__name__)


class GCSToBigQueryOperatorLineageMixin:
    """Mixin class for GCSToBigQueryOperator."""

    def post_execute_prepare_lineage(self: GCSToBigQueryOperator, context: dict):  # type: ignore
        try:
            hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )
        except AirflowException:
            log.exception("Error on creating hook")
            return

        destination = self.destination_project_dataset_table
        try:
            project_id, dataset_id, table_id = hook.split_tablename(
                table_input=destination, default_project_id=hook.project_id  # type: ignore
            )
        except Exception:
            log.exception("Error on parsing table name: '%s'", destination)
            return

        self.inlets.extend(
            [GCSEntity(bucket=self.bucket, path=_source_obj) for _source_obj in self.source_objects]
        )
        self.outlets.append(BigQueryTable(project_id=project_id, dataset_id=dataset_id, table_id=table_id))
