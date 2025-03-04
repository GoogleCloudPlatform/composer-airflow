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
import logging

from airflow.exceptions import AirflowException
from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from airflow.composer.openlineage.extractors.utils import DataprocSQLJobLineageExtractor

log = logging.getLogger(__name__)


class DataprocExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls):
        return ["DataprocSubmitJobOperator"]

    def _execute_extraction(self) -> OperatorLineage:
        return OperatorLineage()

    def extract_on_complete(self, task_instance) -> OperatorLineage:
        """Add what we received after Operator's extract call."""

        from google.api_core.exceptions import NotFound

        from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
        from airflow.composer.openlineage.extractors.utils import xcom_pull

        hook = DataprocHook(
            gcp_conn_id=self.operator.gcp_conn_id, impersonation_chain=self.operator.impersonation_chain
        )
        if hook.project_id is None:
            log.exception("The project_id is missing. Data lineage wasn't reported")
            return OperatorLineage()

        try:
            job_id: str = xcom_pull(task_instance=task_instance)
            job = hook.get_job(
                job_id=job_id, project_id=self.operator.project_id, region=self.operator.region
            )
        except NotFound:
            log.exception(f"The job with id {job_id} wasn't found. Data lineage wasn't reported")
            return OperatorLineage()

        data_lineage_extractor = DataprocSQLJobLineageExtractor(
            job=job, project_id=hook.project_id, location=self.operator.region
        )

        inputs, outputs = [], []
        try:
            inputs, outputs = data_lineage_extractor.data_lineage()
            if not inputs:
                log.info("No sources were detected. Data lineage wasn't reported")
                return OperatorLineage()
            if not outputs:
                log.info("No targets were detected. Data lineage wasn't reported")
                return OperatorLineage()

        except AirflowException as ex:
            log.info(ex)

        return OperatorLineage(inputs=inputs, outputs=outputs)
