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
from unittest.mock import patch

import pytest
from google.api_core.exceptions import GoogleAPICallError

from airflow.composer.data_lineage.entities import BigQueryTable
from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

BIGQUERY_PATH = "airflow.providers.google.cloud.hooks.bigquery"


class TestBigQueryInsertJobOperator:
    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage(self, mock_bigquery_hook):
        def _mock_get_job(project_id, location, job_id):
            assert project_id == "test-project"
            assert location == "location"
            assert job_id == "test-job-id"
            return mock.Mock(
                _properties={
                    "statistics": {
                        "query": {
                            "referencedTables": [
                                {
                                    "projectId": "project-1",
                                    "datasetId": "dataset-1",
                                    "tableId": "table-1",
                                },
                                {
                                    "projectId": "project-2",
                                    "datasetId": "dataset-2",
                                    "tableId": "table-2",
                                },
                                {
                                    "projectId": "project-2",
                                    "tableId": "ephemeral-table",
                                },
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

        job_id_path = "test-project:location:test-job-id"
        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value=job_id_path), map_index=-1)}

        post_execute_prepare_lineage(task, context)

        assert task.inlets == [
            BigQueryTable(
                project_id="project-1",
                dataset_id="dataset-1",
                table_id="table-1",
            )
        ]
        assert task.outlets == [
            BigQueryTable(
                project_id="project-2",
                dataset_id="dataset-2",
                table_id="table-2",
            )
        ]

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_partition(self, mock_bigquery_hook):
        def _mock_get_job(project_id, location, job_id):
            assert project_id == "test-project"
            assert location == "location"
            assert job_id == "test-job-id"
            return mock.Mock(
                _properties={
                    "statistics": {
                        "query": {
                            "referencedTables": [
                                {
                                    "projectId": "project-1",
                                    "datasetId": "dataset-1",
                                    "tableId": "table-1$partition1",
                                },
                                {
                                    "projectId": "project-2",
                                    "datasetId": "dataset-2",
                                    "tableId": "table-2$partition2",
                                },
                                {
                                    "projectId": "project-2",
                                    "tableId": "ephemeral-table$partition3",
                                },
                            ]
                        },
                    },
                    "configuration": {
                        "query": {
                            "destinationTable": {
                                "projectId": "project-2",
                                "datasetId": "dataset-2",
                                "tableId": "table-2$partition4",
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

        job_id_path = "test-project:location:test-job-id"
        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value=job_id_path), map_index=-1)}

        post_execute_prepare_lineage(task, context)

        assert task.inlets == [
            BigQueryTable(
                project_id="project-1",
                dataset_id="dataset-1",
                table_id="table-1",
            )
        ]
        assert task.outlets == [
            BigQueryTable(
                project_id="project-2",
                dataset_id="dataset-2",
                table_id="table-2",
            )
        ]

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_xcom_pull(self, mock_bigquery_hook):
        def _mock_get_job(project_id, location, job_id):
            assert project_id == "test-project"
            assert location == "location"
            assert job_id == "test-job-id"
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

        job_id_path = "test-project:location:test-job-id"
        mock_xcom_pull = mock.Mock(return_value=job_id_path)
        context = {"task_instance": mock.Mock(task_id="test-task-id", xcom_pull=mock_xcom_pull, map_index=-1)}

        post_execute_prepare_lineage(task, context)

        mock_xcom_pull.assert_called_with(task_ids="test-task-id", key="job_id_path")

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_create_hook_error(self, mock_bigquery_hook):
        task = BigQueryInsertJobOperator(task_id="test-task", location="location", configuration={})
        mock_bigquery_hook.side_effect = AirflowNotFoundException
        job_id_path = "test-project:location:test-job-id"
        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value=job_id_path), map_index=-1)}

        post_execute_prepare_lineage(task, context)

        mock_bigquery_hook.assert_called_once_with(
            gcp_conn_id=task.gcp_conn_id,
            impersonation_chain=task.impersonation_chain,
        )
        assert task.inlets == []
        assert task.outlets == []

    @patch("airflow.composer.data_lineage.utils.is_big_query_table_in_sources", autospec=True)
    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_parse_query_error(self, mock_bigquery_hook, mock_table_in_sources):
        def _mock_get_job(project_id, location, job_id):
            assert project_id == "test-project"
            assert location == "location"
            assert job_id == "test-job-id"
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
        mock_table_in_sources.side_effect = RecursionError()

        task.job_id = "test-job-id"

        job_id_path = "test-project:location:test-job-id"
        mock_xcom_pull = mock.Mock(return_value=job_id_path)
        context = {"task_instance": mock.Mock(task_id="test-task-id", xcom_pull=mock_xcom_pull, map_index=-1)}

        post_execute_prepare_lineage(task, context)

        mock_xcom_pull.assert_called_with(task_ids="test-task-id", key="job_id_path")

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_get_job_error(self, mock_bigquery_hook):
        def _mock_get_job(project_id, location, job_id):
            assert project_id == "test-project"
            assert location == "location"
            assert job_id == "test-job-id"
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
        job_id_path = "test-project:location:test-job-id"
        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value=job_id_path), map_index=-1)}

        post_execute_prepare_lineage(task, context)

        assert task.inlets == []
        assert task.outlets == []

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_empty_props(self, mock_bigquery_hook):
        def _mock_get_job(project_id, location, job_id):
            assert project_id == "test-project"
            assert location == "location"
            assert job_id == "test-job-id"
            return mock.Mock(_properties={})

        task = BigQueryInsertJobOperator(
            task_id="test-task",
            configuration={},
            project_id="test-project",
            location="location",
        )
        mock_bigquery_hook.return_value = mock.Mock(get_job=mock.Mock(side_effect=_mock_get_job))
        task.job_id = "test-job-id"
        job_id_path = "test-project:location:test-job-id"
        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value=job_id_path), map_index=-1)}

        post_execute_prepare_lineage(task, context)

        assert task.inlets == []
        assert task.outlets == []

    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_no_job_id(self, mock_bigquery_hook):
        def _mock_get_job(project_id, location, job_id):
            assert project_id == "test-project"
            assert location == "location"
            assert job_id == "test-job-id"
            return mock.Mock(
                _properties={
                    "statistics": {
                        "query": {
                            "referencedTables": [
                                {
                                    "projectId": "project-1",
                                    "datasetId": "dataset-1",
                                    "tableId": "table-1",
                                },
                                {
                                    "projectId": "project-2",
                                    "datasetId": "dataset-2",
                                    "tableId": "table-2",
                                },
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

        job_id_path = "test-project:location:test-job-id"
        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value=job_id_path), map_index=-1)}

        post_execute_prepare_lineage(task, context)

        assert task.inlets == [
            BigQueryTable(project_id="project-1", dataset_id="dataset-1", table_id="table-1")
        ]
        assert task.outlets == [
            BigQueryTable(project_id="project-2", dataset_id="dataset-2", table_id="table-2")
        ]

    @pytest.mark.parametrize(
        "query, job_project_id, default_dataset, expected_in_inlets",
        [
            (
                (
                    "INSERT INTO `project-2.dataset-2.table2`(a, b) "
                    "select table1.a, table2.b from table1, `dataset-2.table2`;"
                ),
                "project-2",
                None,
                True,
            ),
            (
                (
                    "INSERT INTO `project-2.dataset-2.table2`(a, b) "
                    "select table1.a, table2.b from table1, `dataset-2.table2`;"
                ),
                "project-1",
                None,
                False,
            ),
            (
                (
                    "INSERT INTO `project-2.dataset-2.table2`(a, b) "
                    "select table1.a, table2.b from table1, table2;"
                ),
                "project-1",
                {"datasetId": "dataset-2", "projectId": "project-2"},
                True,
            ),
            (
                (
                    "INSERT INTO `project-2.dataset-2.table2`(a, b) "
                    "select table1.a, table2.b from table1, table2;"
                ),
                "project-2",
                {"datasetId": "dataset-2"},
                True,
            ),
            (
                (
                    "INSERT INTO `project-2.dataset-2.table2`(a, b) "
                    "select table1.a, table2.b from table1, table2;"
                ),
                "project-1",
                {"datasetId": "dataset-1"},
                False,
            ),
        ],
    )
    @patch(BIGQUERY_PATH + ".BigQueryHook", autospec=True)
    def test_post_execute_prepare_lineage_source_is_target(
        self, mock_bigquery_hook, query, job_project_id, default_dataset, expected_in_inlets
    ):
        def _mock_get_job(project_id, location, job_id):
            assert project_id == "test-project"
            assert location == "location"
            assert job_id == "test-job-id"
            properties = {
                "statistics": {
                    "query": {
                        "referencedTables": [
                            {
                                "projectId": "project-1",
                                "datasetId": "dataset-1",
                                "tableId": "table1",
                            },
                            {
                                "projectId": "project-1",
                                "datasetId": "dataset-1",
                                "tableId": "table2",
                            },
                            {
                                "projectId": "project-2",
                                "datasetId": "dataset-2",
                                "tableId": "table2",
                            },
                        ]
                    },
                },
                "configuration": {
                    "query": {
                        "destinationTable": {
                            "projectId": "project-2",
                            "datasetId": "dataset-2",
                            "tableId": "table2",
                        },
                        "query": query,
                    },
                },
                "jobReference": {"projectId": job_project_id},
            }

            if default_dataset:
                properties["configuration"]["query"]["defaultDataset"] = default_dataset
            return mock.Mock(
                _properties=properties,
            )

        task = BigQueryInsertJobOperator(
            task_id="test-task",
            configuration={},
            project_id="test-project",
            location="location",
        )
        mock_bigquery_hook.return_value = mock.Mock(
            location="location",
            project_id="project-1",
            get_job=mock.Mock(side_effect=_mock_get_job),
        )
        task.job_id = "test-job-id"

        job_id_path = "test-project:location:test-job-id"
        context = {"task_instance": mock.Mock(xcom_pull=mock.Mock(return_value=job_id_path), map_index=-1)}

        post_execute_prepare_lineage(task, context)

        assert task.inlets == [
            BigQueryTable(project_id="project-1", dataset_id="dataset-1", table_id="table1"),
            BigQueryTable(project_id="project-1", dataset_id="dataset-1", table_id="table2"),
        ] + (
            [BigQueryTable(project_id="project-2", dataset_id="dataset-2", table_id="table2")]
            if expected_in_inlets
            else []
        )
        assert task.outlets == [
            BigQueryTable(project_id="project-2", dataset_id="dataset-2", table_id="table2")
        ]
