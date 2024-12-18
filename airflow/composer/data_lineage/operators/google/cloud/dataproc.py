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
from typing import TYPE_CHECKING, Sequence

from airflow.composer.data_lineage.utils import xcom_pull

if TYPE_CHECKING:
    from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

from airflow.exceptions import AirflowException
from airflow.composer.openlineage.extractors.utils import DataprocSQLJobLineageExtractor

log = logging.getLogger(__name__)


class DataprocSubmitJobOperatorLineageMixin:
    """Mixin class for DataprocSubmitJobOperator."""

    def post_execute_prepare_lineage(self: DataprocSubmitJobOperator, context: dict):  # type: ignore

        from google.api_core.exceptions import NotFound

        from airflow.providers.google.cloud.hooks.dataproc import DataprocHook

        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        if hook.project_id is None:
            log.exception("The project_id is missing. Data lineage wasn't reported")
            return

        try:
            task_instance = context["task_instance"]
            job_id: str = xcom_pull(task_instance=task_instance)
            job = hook.get_job(job_id=job_id, project_id=self.project_id, region=self.region)
        except NotFound:
            log.exception(f"The job with id {job_id} wasn't found. Data lineage wasn't reported")
            return
        except KeyError:
            log.exception("The context didn't include task_instance. Data lineage wasn't reported")
            return

        data_lineage_extractor = DataprocSQLJobLineageExtractor(
            job=job, project_id=hook.project_id, location=self.region
        )

        try:
            inlets, outlets = data_lineage_extractor.data_lineage(output_type="metastore_table")
            if not inlets:
                log.info("No sources were detected. Data lineage wasn't reported")
                return
            if not outlets:
                log.info("No targets were detected. Data lineage wasn't reported")
                return

            self.inlets.extend(inlets)
            self.outlets.extend(outlets)
        except AirflowException as ex:
            log.info(ex)
            return
