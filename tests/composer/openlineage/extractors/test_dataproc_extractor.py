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
from unittest import mock

from airflow.composer.openlineage.extractors.dataproc_extractor import DataprocExtractor
from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.common.compat.openlineage.facet import Dataset
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

INPUTS = [Dataset(namespace="dataproc_metastore", name="inputtable")]
OUTPUTS = [Dataset(namespace="dataproc_metastore", name="inputtable")]
TEST_REGION = "test_region"
TEST_TASK_ID = "test_task_id"
TEST_PROJECT_ID = "test_project_id"
TEST_GCP_CONN_ID = "test_gcp_conn_id"
TEST_IMPERSONATION_CHAIN = "test_impersonation_chain"


class TestDataprocExtractor:

    def test_extractor_linking(self):
        assert DataprocExtractor.get_operator_classnames() == ["DataprocSubmitJobOperator"]

    @mock.patch(
        "airflow.composer.openlineage.extractors.dataproc_extractor.DataprocSQLJobLineageExtractor",
        autospec=True,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook", autospec=True)
    @mock.patch("airflow.composer.openlineage.extractors.utils.xcom_pull", autospec=True)
    def test_extract_on_complete(self, mock_xcom_pull, mock_dataproc_hook, mock_dataproc_sql_extractor):

        operator = DataprocSubmitJobOperator(
            task_id=TEST_TASK_ID,
            job=mock.MagicMock(),
            region=TEST_REGION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_dataproc_sql_extractor.return_value.data_lineage.return_value = (INPUTS, OUTPUTS)
        task_instance = mock.MagicMock()

        metadata_on_complete = DataprocExtractor(operator).extract_on_complete(task_instance=task_instance)

        expected_task_metadata = OperatorLineage(
            inputs=INPUTS,
            outputs=OUTPUTS,
        )

        assert metadata_on_complete == expected_task_metadata

    @mock.patch(
        "airflow.composer.openlineage.extractors.dataproc_extractor.DataprocSQLJobLineageExtractor",
        autospec=True,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook", autospec=True)
    @mock.patch("airflow.composer.openlineage.extractors.utils.xcom_pull", autospec=True)
    @mock.patch("airflow.composer.openlineage.extractors.dataproc_extractor.log", autospec=True)
    def test_extract_on_complete_no_project_id(
        self, mock_log, mock_xcom_pull, mock_dataproc_hook, mock_dataproc_sql_extractor
    ):

        operator = DataprocSubmitJobOperator(
            task_id=TEST_TASK_ID,
            job=mock.MagicMock(),
            region=TEST_REGION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_dataproc_sql_extractor.return_value.data_lineage.return_value = (INPUTS, OUTPUTS)
        task_instance = mock.MagicMock()
        mock_dataproc_hook_inst = mock_dataproc_hook.return_value
        mock_dataproc_hook_inst.project_id = None

        metadata_on_complete = DataprocExtractor(operator).extract_on_complete(task_instance=task_instance)

        expected_task_metadata = OperatorLineage()

        assert metadata_on_complete == expected_task_metadata
        mock_log.exception.assert_called_once_with("The project_id is missing. Data lineage wasn't reported")
