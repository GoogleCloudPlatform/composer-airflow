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
from typing import TYPE_CHECKING

from airflow.composer.data_lineage.utils import exclude_bigquery_partition

if TYPE_CHECKING:
    from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator

from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)


class BigQueryToBigQueryOperatorLineageMixin:
    """Mixin class for BigQueryToBigQueryOperator."""

    def post_execute_prepare_lineage(self: BigQueryToBigQueryOperator, context: dict):  # type: ignore
        from airflow.composer.data_lineage.entities import BigQueryTable
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        try:
            hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )
        except AirflowException as airflow_exception:
            log.exception("Error on creating hook: %s", airflow_exception)
            return

        sources = self.source_project_dataset_tables
        if isinstance(sources, str):
            sources = [sources]

        inlets = []
        for table_name in sources:
            try:
                project_id, dataset_id, table_id = hook.split_tablename(
                    table_input=table_name, default_project_id=hook.project_id  # type: ignore
                )
            except Exception:
                log.exception('Error on parsing table name: "%s"', table_name)
                return
            table_id = exclude_bigquery_partition(table_id=table_id)
            inlets.append(BigQueryTable(project_id=project_id, dataset_id=dataset_id, table_id=table_id))

        outlets = []
        try:
            project_id, dataset_id, table_id = hook.split_tablename(
                table_input=self.destination_project_dataset_table,
                default_project_id=hook.project_id,  # type: ignore
            )
        except Exception:
            log.exception('Error on parsing table name: "%s"', self.destination_project_dataset_table)
            return
        table_id = exclude_bigquery_partition(table_id=table_id)
        outlets.append(BigQueryTable(project_id=project_id, dataset_id=dataset_id, table_id=table_id))

        self.inlets.extend(inlets)
        self.outlets.extend(outlets)
