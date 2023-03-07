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
import logging
from typing import Dict

from google.api_core.exceptions import GoogleAPICallError

from airflow.composer.data_lineage.entities import BigQueryTable
from airflow.composer.data_lineage.utils import exclude_outlet
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

log = logging.getLogger(__name__)


class BigQueryInsertJobOperatorLineageMixin:
    """Mixin class for BigQueryInsertJobOperator."""

    def post_execute_prepare_lineage(self, context: Dict):
        task_instance = context["task_instance"]
        job_id = task_instance.xcom_pull(key="job_id")

        try:
            hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
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

        input_tables = props.get("statistics", {}).get("query", {}).get("referencedTables", [])
        inlets = [
            BigQueryTable(
                project_id=input_table["projectId"],
                dataset_id=input_table["datasetId"],
                table_id=input_table["tableId"],
            )
            for input_table in input_tables
        ]

        output_table = props.get("configuration", {}).get("query", {}).get("destinationTable")
        if output_table:
            outlet = BigQueryTable(
                project_id=output_table["projectId"],
                dataset_id=output_table["datasetId"],
                table_id=output_table["tableId"],
            )
            self.outlets.append(outlet)

            inlets = exclude_outlet(inlets=inlets, outlet=outlet)

        self.inlets.extend(inlets)


class BigQueryExecuteQueryOperatorLineageMixin:
    """Mixin class for BigQueryExecuteQueryOperator."""

    def post_execute_prepare_lineage(self, context: Dict):
        task_instance = context["task_instance"]
        job_id = task_instance.xcom_pull(key="job_id")

        try:
            hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
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

        input_tables = props.get("statistics", {}).get("query", {}).get("referencedTables", [])
        inlets = [
            BigQueryTable(
                project_id=input_table["projectId"],
                dataset_id=input_table["datasetId"],
                table_id=input_table["tableId"],
            )
            for input_table in input_tables
        ]

        output_table = props.get("configuration", {}).get("query", {}).get("destinationTable")
        if output_table:
            outlet = BigQueryTable(
                project_id=output_table["projectId"],
                dataset_id=output_table["datasetId"],
                table_id=output_table["tableId"],
            )
            self.outlets.append(outlet)

            inlets = exclude_outlet(inlets=inlets, outlet=outlet)

        self.inlets.extend(inlets)
