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
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from airflow.composer.data_lineage.utils import xcom_pull

if TYPE_CHECKING:
    from airflow.composer.data_lineage.entities import BigQueryTable
    from airflow.providers.google.cloud.operators.bigquery import (
        BigQueryExecuteQueryOperator,
        BigQueryInsertJobOperator,
    )

log = logging.getLogger(__name__)


def _should_exclude_outlet(props: dict, outlet: BigQueryTable):
    from sqllineage.exceptions import SQLLineageException

    from airflow.composer.data_lineage.utils import is_big_query_table_in_sources

    query = props.get("configuration", {}).get("query", {}).get("query", "")
    default_dataset = (
        props.get("configuration", {}).get("query", {}).get("defaultDataset", {}).get("datasetId")
    )
    job_project_id = props.get("jobReference", {}).get("projectId")
    default_project = (
        props.get("configuration", {})
        .get("query", {})
        .get("defaultDataset", {})
        .get("projectId", job_project_id)
    )

    try:
        return not is_big_query_table_in_sources(query, outlet, default_dataset, default_project)
    except (SQLLineageException, RecursionError):
        log.exception("Error parsing sql query. Failed to check if the outlet is also a valid inlet.")
        return True
    except Exception:
        # We catch all exceptions here as this is just a corner case and we shouldn't fail because of this.
        log.exception("Error parsing sql query. Failed to check if the outlet is also a valid inlet.")
        return True


class BigQueryInsertJobOperatorLineageMixin:
    """Mixin class for BigQueryInsertJobOperator."""

    def post_execute_prepare_lineage(self: BigQueryInsertJobOperator, context: dict):  # type: ignore

        from google.api_core.exceptions import GoogleAPICallError

        from airflow.composer.data_lineage.entities import BigQueryTable
        from airflow.composer.data_lineage.utils import exclude_outlet
        from airflow.exceptions import AirflowNotFoundException
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        task_instance = context["task_instance"]
        job_id_path: str = xcom_pull(task_instance=task_instance, key="job_id_path")
        if not job_id_path:
            log.exception("No job_id_path found.")
            return

        job_id = job_id_path.split(":")[-1]

        try:
            hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            )
        except AirflowNotFoundException:
            log.exception("Error on creating BigQuery hook")
            return

        try:
            job = hook.get_job(
                project_id=self.project_id,
                location=self.location,
                job_id=job_id,
            )
        except GoogleAPICallError:
            # Catch both client and server errors.
            log.exception("Error on fetching BigQuery job")
            return

        props = job._properties

        # We use referencedTables as it's the most reliable way to get all tables used in the query.
        # This contains the target table (if any) so we take care of excluding it if necessary.
        # Ephemeral tables, defined with tableDefinitions, don't include "datasetId", so we ignore them.
        input_tables = props.get("statistics", {}).get("query", {}).get("referencedTables", [])
        inlets = [
            BigQueryTable(
                project_id=input_table["projectId"],
                dataset_id=input_table["datasetId"],
                table_id=input_table["tableId"],
            )
            for input_table in input_tables
            if input_table.get("datasetId")
        ]

        output_table = props.get("configuration", {}).get("query", {}).get("destinationTable")
        if output_table:
            outlet = BigQueryTable(
                project_id=output_table["projectId"],
                dataset_id=output_table["datasetId"],
                table_id=output_table["tableId"],
            )
            self.outlets.append(outlet)

            if _should_exclude_outlet(props, outlet):
                inlets = exclude_outlet(inlets=inlets, outlet=outlet)

        self.inlets.extend(inlets)


class BigQueryExecuteQueryOperatorLineageMixin:
    """Mixin class for BigQueryExecuteQueryOperator."""

    def post_execute_prepare_lineage(self: BigQueryExecuteQueryOperator, context: dict):  # type: ignore

        from google.api_core.exceptions import GoogleAPICallError

        from airflow.composer.data_lineage.entities import BigQueryTable
        from airflow.composer.data_lineage.utils import exclude_outlet
        from airflow.exceptions import AirflowNotFoundException
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        task_instance = context["task_instance"]
        job_id_path: str = task_instance.xcom_pull(task_ids=task_instance.task_id, key="job_id_path")
        if not job_id_path:
            log.exception("No job_id_path found.")
            return

        job_id = job_id_path.split(":")[-1]

        try:
            hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            )
        except AirflowNotFoundException:
            log.exception("Error on creating BigQuery hook")
            return

        try:
            job = hook.get_job(job_id=job_id, location=self.location)
        except GoogleAPICallError:
            # Catch both client and server errors.
            log.exception("Error on fetching BigQuery job")
            return

        props = job._properties

        # We use referencedTables as it's the most reliable way to get all tables used in the query.
        # This contains the target table (if any) so we take care of excluding it if necessary.
        # Ephemeral tables, defined with tableDefinitions, don't include "datasetId", so we ignore them.
        input_tables = props.get("statistics", {}).get("query", {}).get("referencedTables", [])
        inlets = [
            BigQueryTable(
                project_id=input_table["projectId"],
                dataset_id=input_table["datasetId"],
                table_id=input_table["tableId"],
            )
            for input_table in input_tables
            if input_table.get("datasetId")
        ]

        output_table = props.get("configuration", {}).get("query", {}).get("destinationTable")
        if output_table:
            outlet = BigQueryTable(
                project_id=output_table["projectId"],
                dataset_id=output_table["datasetId"],
                table_id=output_table["tableId"],
            )
            self.outlets.append(outlet)

            if _should_exclude_outlet(props, outlet):
                inlets = exclude_outlet(inlets=inlets, outlet=outlet)

        self.inlets.extend(inlets)
