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

import unittest
from unittest import mock
from unittest.mock import patch

from google.api_core.exceptions import GoogleAPICallError

from airflow.composer.data_lineage.entities import BigQueryTable
from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)

BIGQUERY_PATH = "airflow.composer.data_lineage.operators.google.cloud.bigquery"


class TestBigQueryInsertJobOperator(unittest.TestCase):
    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage(self, mock_bigquery_hook):
        def _mock_get_job(project_id, location, job_id):
            self.assertEqual(project_id, "test-project")
            self.assertEqual(location, "location")
            self.assertEqual(job_id, "test-job-id")
            return mock.Mock(
                _properties={
                    "statistics": {
                        "query": {
                            "referencedTables": [
                                {
                                    "projectId": "project-1",
                                    "datasetId": "dataset-1",
                                    "tableId": "table-1",
                                }
                            ]
                        },
                    },
                    "configuration": {
                        "query": {
                            "destinationTable": {
                                "projectId": "project-2",
                                "datasetId": "dataset-2",
                                "tableId": "table-2",
                            },
                        },
                    },
                },
            )

        task = BigQueryInsertJobOperator(
            task_id="test-task",
            configuration={},
            project_id="test-project",
            location="location",
        )
        mock_bigquery_hook.return_value = mock.Mock(
            get_job=mock.Mock(side_effect=_mock_get_job),
        )
        task.job_id = "test-job-id"
        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value="test-job-id"))}

        post_execute_prepare_lineage(task, context)

        self.assertEqual(
            task.inlets,
            [
                BigQueryTable(
                    project_id="project-1",
                    dataset_id="dataset-1",
                    table_id="table-1",
                )
            ],
        )
        self.assertEqual(
            task.outlets,
            [
                BigQueryTable(
                    project_id="project-2",
                    dataset_id="dataset-2",
                    table_id="table-2",
                )
            ],
        )

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_xcom_pull(self, mock_bigquery_hook):
        def _mock_get_job(project_id, location, job_id):
            self.assertEqual(project_id, "test-project")
            self.assertEqual(location, "location")
            self.assertEqual(job_id, "test-job-id")
            return mock.Mock(
                _properties={
                    "statistics": {
                        "query": {"referencedTables": []},
                    },
                    "configuration": {
                        "query": {
                            "destinationTable": {
                                "projectId": "project-2",
                                "datasetId": "dataset-2",
                                "tableId": "table-2",
                            },
                        },
                    },
                },
            )

        task = BigQueryInsertJobOperator(
            task_id="test-task",
            configuration={},
            project_id="test-project",
            location="location",
        )
        mock_bigquery_hook.return_value = mock.Mock(
            get_job=mock.Mock(side_effect=_mock_get_job),
        )
        task.job_id = "test-job-id"
        mock_xcom_pull = mock.Mock(return_value="test-job-id")
        context = {"task_instance": mock.Mock(xcom_pull=mock_xcom_pull)}

        post_execute_prepare_lineage(task, context)

        mock_xcom_pull.assert_called_with(key="job_id")

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_create_hook_error(self, mock_bigquery_hook):
        task = BigQueryExecuteQueryOperator(sql="SQL", task_id="test-task", location="location")
        mock_bigquery_hook.side_effect = AirflowNotFoundException
        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value="test-job-id"))}

        post_execute_prepare_lineage(task, context)

        mock_bigquery_hook.assert_called_once_with(
            gcp_conn_id=task.gcp_conn_id,
            impersonation_chain=task.impersonation_chain,
        )
        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_get_job_error(self, mock_bigquery_hook):
        def _mock_get_job(project_id, location, job_id):
            self.assertEqual(project_id, "test-project")
            self.assertEqual(location, "location")
            self.assertEqual(job_id, "test-job-id")
            raise GoogleAPICallError("error")

        task = BigQueryInsertJobOperator(
            task_id="test-task",
            configuration={},
            project_id="test-project",
            location="location",
        )
        mock_bigquery_hook.return_value = mock.Mock(
            get_job=mock.Mock(side_effect=_mock_get_job),
        )
        task.job_id = "test-job-id"
        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value="test-job-id"))}

        post_execute_prepare_lineage(task, context)

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_empty_props(self, mock_bigquery_hook):
        def _mock_get_job(project_id, location, job_id):
            self.assertEqual(project_id, "test-project")
            self.assertEqual(location, "location")
            self.assertEqual(job_id, "test-job-id")
            return mock.Mock(_properties={})

        task = BigQueryInsertJobOperator(
            task_id="test-task",
            configuration={},
            project_id="test-project",
            location="location",
        )
        mock_bigquery_hook.return_value = mock.Mock(get_job=mock.Mock(side_effect=_mock_get_job))
        task.job_id = "test-job-id"
        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value="test-job-id"))}

        post_execute_prepare_lineage(task, context)

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])


class TestBigQueryExecuteQueryOperator(unittest.TestCase):
    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage(self, mock_bigquery_hook):
        def _mock_get_job(job_id, location):
            self.assertEqual(job_id, "test-job-id")
            self.assertEqual(location, "location")
            return mock.Mock(
                _properties={
                    "statistics": {
                        "query": {
                            "referencedTables": [
                                {
                                    "projectId": "project-1",
                                    "datasetId": "dataset-1",
                                    "tableId": "table-1",
                                }
                            ]
                        },
                    },
                    "configuration": {
                        "query": {
                            "destinationTable": {
                                "projectId": "project-2",
                                "datasetId": "dataset-2",
                                "tableId": "table-2",
                            },
                        },
                    },
                },
            )

        task = BigQueryExecuteQueryOperator(sql="SQL", task_id="test-task", location="location")
        mock_bigquery_hook.return_value = mock.Mock(
            location="location",
            project_id="project-1",
            get_job=mock.Mock(side_effect=_mock_get_job),
        )
        task.job_id = "test-job-id"

        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value="test-job-id"))}

        post_execute_prepare_lineage(task, context)

        self.assertEqual(
            task.inlets,
            [
                BigQueryTable(
                    project_id="project-1",
                    dataset_id="dataset-1",
                    table_id="table-1",
                )
            ],
        )
        self.assertEqual(
            task.outlets,
            [
                BigQueryTable(
                    project_id="project-2",
                    dataset_id="dataset-2",
                    table_id="table-2",
                )
            ],
        )

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_xcom_pull(self, mock_bigquery_hook):
        def _mock_get_job(project_id, location, job_id):
            self.assertEqual(project_id, "test-project")
            self.assertEqual(location, "location")
            self.assertEqual(job_id, "test-job-id")
            return mock.Mock(
                _properties={
                    "statistics": {
                        "query": {"referencedTables": []},
                    },
                    "configuration": {
                        "query": {
                            "destinationTable": {
                                "projectId": "project-2",
                                "datasetId": "dataset-2",
                                "tableId": "table-2",
                            },
                        },
                    },
                },
            )

        task = BigQueryInsertJobOperator(
            task_id="test-task",
            configuration={},
            project_id="test-project",
            location="location",
        )
        mock_bigquery_hook.return_value = mock.Mock(
            get_job=mock.Mock(side_effect=_mock_get_job),
        )
        task.job_id = "test-job-id"
        mock_xcom_pull = mock.Mock(return_value="test-job-id")
        context = {"task_instance": mock.Mock(xcom_pull=mock_xcom_pull)}

        post_execute_prepare_lineage(task, context)

        mock_xcom_pull.assert_called_with(key="job_id")

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_create_hook_error(self, mock_bigquery_hook):
        task = BigQueryExecuteQueryOperator(sql="SQL", task_id="test-task", location="location")
        mock_bigquery_hook.side_effect = AirflowNotFoundException
        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value="test-job-id"))}

        post_execute_prepare_lineage(task, context)

        mock_bigquery_hook.assert_called_once_with(
            gcp_conn_id=task.gcp_conn_id,
            impersonation_chain=task.impersonation_chain,
        )
        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_get_job_error(self, mock_bigquery_hook):
        def _mock_get_job(job_id, location):
            self.assertEqual(job_id, "test-job-id")
            self.assertEqual(location, "location")
            raise GoogleAPICallError("error")

        task = BigQueryExecuteQueryOperator(sql="SQL", task_id="test-task", location="location")
        mock_bigquery_hook.return_value = mock.Mock(
            location="location",
            get_job=mock.Mock(side_effect=_mock_get_job),
        )
        task.job_id = "test-job-id"

        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value="test-job-id"))}

        post_execute_prepare_lineage(task, context)

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_empty_props(self, mock_bigquery_hook):
        def _mock_get_job(job_id, location):
            self.assertEqual(job_id, "test-job-id")
            self.assertEqual(location, "location")
            return mock.Mock(_properties={})

        task = BigQueryExecuteQueryOperator(sql="SQL", task_id="test-task", location="location")
        mock_bigquery_hook.return_value = mock.Mock(
            location="location",
            get_job=mock.Mock(side_effect=_mock_get_job),
        )
        task.job_id = "test-job-id"

        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value="test-job-id"))}

        post_execute_prepare_lineage(task, context)

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])
