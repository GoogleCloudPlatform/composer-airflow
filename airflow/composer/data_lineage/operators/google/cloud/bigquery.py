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

log = logging.getLogger(__name__)


class BigQueryInsertJobOperatorLineageMixin:
    """Mixin class for BigQueryInsertJobOperator."""

    def post_execute_prepare_lineage(self, context: Dict):
        hook = self.hook
        bigquery_job_id = self.job_id

        client = hook.get_client(
            project_id=hook.project_id,
            location=hook.location,
        )

        try:
            job = client.get_job(job_id=bigquery_job_id)
        except GoogleAPICallError:
            # Catch both client and server errors.
            log.exception("Error on fetching BigQuery job")
            return

        props = job._properties

        input_tables = props.get("statistics", {}).get("query", {}).get("referencedTables", [])
        for input_table in input_tables:
            inlet = BigQueryTable(
                project_id=input_table["projectId"],
                dataset_id=input_table["datasetId"],
                table_id=input_table["tableId"],
            )
            self.inlets.append(inlet)

        output_table = props.get("configuration", {}).get("query", {}).get("destinationTable")
        if output_table:
            outlet = BigQueryTable(
                project_id=output_table["projectId"],
                dataset_id=output_table["datasetId"],
                table_id=output_table["tableId"],
            )
            self.outlets.append(outlet)

            # TODO: fix inlets containing outlet and remove this temporary workaround. We have this workaround
            # for now as it is rather an edge case when inlets containing outlet.
            self.inlets = [_inlet for _inlet in self.inlets if _inlet != outlet]
