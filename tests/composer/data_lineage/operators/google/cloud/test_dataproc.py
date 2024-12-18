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

from unittest import mock
from unittest.mock import MagicMock, PropertyMock, call

from google.api_core.exceptions import NotFound

from airflow.exceptions import AirflowException
from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

TEST_PROJECT_ID = "test_project_id"
TEST_LOCATION = "test_location"
TEST_DATAPROC_CLUSTER = "test_cluster"

SQL_TABLE = "table"
SQL_SELECT_FROM_TABLE = f"SELECT * FROM {SQL_TABLE}"

HIVE_JOB = {
    "reference": {"project_id": TEST_PROJECT_ID},
    "placement": {"cluster_name": TEST_DATAPROC_CLUSTER},
    "hive_job": {"query_list": {"queries": [SQL_SELECT_FROM_TABLE]}},
}


class TestDataprocSubmitJobOperatorLineageMixin:
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook")
    @mock.patch(
        "airflow.composer.data_lineage.operators.google.cloud.dataproc.DataprocSQLJobLineageExtractor"
    )
    def test_post_execute_prepare_lineage(self, mock_extractor, mock_hook):
        expected_inlets = [MagicMock(), MagicMock()]
        expected_outlets = [MagicMock(), MagicMock()]
        mock_extractor.return_value.data_lineage.return_value = (expected_inlets, expected_outlets)
        job_id = "test-job-id"
        mock_xcom_pull = mock.Mock(return_value=job_id)
        context = {"task_instance": mock.Mock(task_id="hive_task", xcom_pull=mock_xcom_pull, map_index=-1)}

        task = DataprocSubmitJobOperator(
            task_id="hive_task", job=HIVE_JOB, region=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )
        post_execute_prepare_lineage(task=task, context=context)

        assert task.inlets == expected_inlets
        assert task.outlets == expected_outlets

    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook")
    def test_post_execute_prepare_lineage_no_project_id(self, mock_hook):
        mock_hook.return_value.project_id = None
        job_id = "test-job-id"
        mock_xcom_pull = mock.Mock(return_value=job_id)
        context = {"task_instance": mock.Mock(task_id="hive_task", xcom_pull=mock_xcom_pull, map_index=-1)}

        task = DataprocSubmitJobOperator(task_id="hive_task", job=HIVE_JOB, region=TEST_LOCATION)
        post_execute_prepare_lineage(task=task, context=context)

        assert task.inlets == []
        assert task.outlets == []

    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook")
    def test_post_execute_prepare_lineage_no_job(self, mock_hook):
        m_hook = MagicMock(project_id=TEST_PROJECT_ID)
        m_hook.get_job.side_effect = NotFound(message="message")
        mock_hook.return_value = m_hook
        job_id = "test-job-id"
        mock_xcom_pull = mock.Mock(return_value=job_id)
        context = {"task_instance": mock.Mock(task_id="hive_task", xcom_pull=mock_xcom_pull, map_index=-1)}

        task = DataprocSubmitJobOperator(
            task_id="hive_task", job=HIVE_JOB, region=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )
        post_execute_prepare_lineage(task=task, context=context)

        assert task.inlets == []
        assert task.outlets == []

    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook")
    @mock.patch(
        "airflow.composer.data_lineage.operators.google.cloud.dataproc.DataprocSQLJobLineageExtractor"
    )
    def test_post_execute_prepare_lineage_no_inlets(self, mock_extractor, mock_hook):
        inlets = []
        outlets = [MagicMock(), MagicMock()]
        mock_extractor.return_value.data_lineage.return_value = (inlets, outlets)
        job_id = "test-job-id"
        mock_xcom_pull = mock.Mock(return_value=job_id)
        context = {"task_instance": mock.Mock(task_id="hive_task", xcom_pull=mock_xcom_pull, map_index=-1)}

        task = DataprocSubmitJobOperator(
            task_id="hive_task", job=HIVE_JOB, region=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )
        post_execute_prepare_lineage(task=task, context=context)

        assert task.inlets == []
        assert task.outlets == []

    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook")
    @mock.patch(
        "airflow.composer.data_lineage.operators.google.cloud.dataproc.DataprocSQLJobLineageExtractor"
    )
    def test_post_execute_prepare_lineage_no_outlets(self, mock_extractor, mock_hook):
        inlets = [MagicMock(), MagicMock()]
        outlets = []
        mock_extractor.return_value.data_lineage.return_value = (inlets, outlets)
        job_id = "test-job-id"
        mock_xcom_pull = mock.Mock(return_value=job_id)
        context = {"task_instance": mock.Mock(task_id="hive_task", xcom_pull=mock_xcom_pull, map_index=-1)}

        task = DataprocSubmitJobOperator(
            task_id="hive_task", job=HIVE_JOB, region=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )
        post_execute_prepare_lineage(task=task, context=context)

        assert task.inlets == []
        assert task.outlets == []

    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook")
    @mock.patch(
        "airflow.composer.data_lineage.operators.google.cloud.dataproc.DataprocSQLJobLineageExtractor"
    )
    def test_post_execute_prepare_lineage_airflow_exception(self, mock_extractor, mock_hook):
        mock_extractor.return_value.data_lineage.side_effect = AirflowException
        job_id = "test-job-id"
        mock_xcom_pull = mock.Mock(return_value=job_id)
        context = {"task_instance": mock.Mock(task_id="hive_task", xcom_pull=mock_xcom_pull, map_index=-1)}

        task = DataprocSubmitJobOperator(
            task_id="hive_task", job=HIVE_JOB, region=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )
        post_execute_prepare_lineage(task=task, context=context)

        assert task.inlets == []
        assert task.outlets == []
