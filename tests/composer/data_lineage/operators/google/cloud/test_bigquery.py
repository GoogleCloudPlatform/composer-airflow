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
import unittest
from unittest import mock

from google.api_core.exceptions import GoogleAPICallError

from airflow.composer.data_lineage.entities import BigQueryTable
from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


class TestBigquery(unittest.TestCase):
    def test_post_execute_prepare_lineage(self):
        def _mock_get_job(job_id):
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

        _mock_client = mock.Mock(get_job=mock.Mock(side_effect=_mock_get_job))

        def _mock_get_client(project_id, location):
            self.assertEqual(project_id, "test-project")
            self.assertEqual(location, "location")
            return _mock_client

        task = BigQueryInsertJobOperator(task_id="test-task", configuration={})
        task.hook = mock.Mock(
            project_id="test-project",
            location="location",
            get_client=mock.Mock(side_effect=_mock_get_client),
        )
        task.job_id = "test-job-id"

        post_execute_prepare_lineage(task, {})

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

    def test_post_execute_prepare_lineage_get_job_error(self):
        def _mock_get_job(job_id):
            self.assertEqual(job_id, "test-job-id")
            raise GoogleAPICallError("error")

        _mock_client = mock.Mock(get_job=mock.Mock(side_effect=_mock_get_job))

        def _mock_get_client(project_id, location):
            self.assertEqual(project_id, "test-project")
            self.assertEqual(location, "location")
            return _mock_client

        task = BigQueryInsertJobOperator(task_id="test-task", configuration={})
        task.hook = mock.Mock(
            project_id="test-project",
            location="location",
            get_client=mock.Mock(side_effect=_mock_get_client),
        )
        task.job_id = "test-job-id"

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    def test_post_execute_prepare_lineage_empty_props(self):
        def _mock_get_job(job_id):
            self.assertEqual(job_id, "test-job-id")
            return mock.Mock(_properties={})

        _mock_client = mock.Mock(get_job=mock.Mock(side_effect=_mock_get_job))

        def _mock_get_client(project_id, location):
            self.assertEqual(project_id, "test-project")
            self.assertEqual(location, "location")
            return _mock_client

        task = BigQueryInsertJobOperator(task_id="test-task", configuration={})
        task.hook = mock.Mock(
            project_id="test-project",
            location="location",
            get_client=mock.Mock(side_effect=_mock_get_client),
        )
        task.job_id = "test-job-id"

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])
