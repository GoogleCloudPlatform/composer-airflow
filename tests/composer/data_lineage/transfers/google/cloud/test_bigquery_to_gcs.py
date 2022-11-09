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
import unittest
from unittest.mock import PropertyMock, patch

from parameterized import parameterized_class

from airflow import AirflowException
from airflow.composer.data_lineage.entities import BigQueryTable, GCSEntity
from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.composer.data_lineage.transfers.google.cloud.bigquery_to_gcs import BigQueryHook
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

TASK_ID = "test-task_id"
TARGET_BUCKET_NAME = "target_bucket"
SOURCE_PROJECT_ID = "source_project"
SOURCE_DATASET_ID = "source_dataset"
SOURCE_TABLE_ID = "source_table"
LOCATION = "us"


@parameterized_class(
    [
        {"operator": BigQueryToGCSOperator},
        {"operator": BigQueryToCloudStorageOperator},
    ]
)
class TestBigQueryToGCS(unittest.TestCase):
    @patch.object(BigQueryHook, "project_id", new_callable=PropertyMock)
    def test_post_execute_prepare_lineage_single_target(self, mock_project_id):
        task = self.operator(
            task_id=TASK_ID,
            source_project_dataset_table=f"{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.{SOURCE_TABLE_ID}",
            destination_cloud_storage_uris=[f"gs://{TARGET_BUCKET_NAME}/test.csv"],
            location=LOCATION,
        )

        post_execute_prepare_lineage(task, {})

        expected_inlets = [
            BigQueryTable(
                project_id=SOURCE_PROJECT_ID, dataset_id=SOURCE_DATASET_ID, table_id=SOURCE_TABLE_ID
            ),
        ]
        self.assertEqual(task.inlets, expected_inlets)

        expected_outlets = [
            GCSEntity(bucket=TARGET_BUCKET_NAME, path="test.csv"),
        ]
        self.assertEqual(task.outlets, expected_outlets)

    @patch.object(BigQueryHook, "project_id", new_callable=PropertyMock)
    def test_post_execute_prepare_lineage_target_with_wildcard(self, mock_project_id):
        task = self.operator(
            task_id=TASK_ID,
            source_project_dataset_table=f"{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.{SOURCE_TABLE_ID}",
            destination_cloud_storage_uris=[f"gs://{TARGET_BUCKET_NAME}/tmp/test-*.csv"],
            location=LOCATION,
        )

        post_execute_prepare_lineage(task, {})

        expected_inlets = [
            BigQueryTable(
                project_id=SOURCE_PROJECT_ID, dataset_id=SOURCE_DATASET_ID, table_id=SOURCE_TABLE_ID
            ),
        ]
        self.assertEqual(task.inlets, expected_inlets)

        expected_outlets = [GCSEntity(bucket=TARGET_BUCKET_NAME, path="tmp/test-*.csv")]
        self.assertEqual(task.outlets, expected_outlets)

    @patch.object(BigQueryHook, "project_id", new_callable=PropertyMock)
    def test_post_execute_prepare_lineage_multiple_targets(self, mock_project_id):
        task = self.operator(
            task_id=TASK_ID,
            source_project_dataset_table=f"{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.{SOURCE_TABLE_ID}",
            destination_cloud_storage_uris=[
                f"gs://{TARGET_BUCKET_NAME}/test.csv",
                f"gs://{TARGET_BUCKET_NAME}/tmp/test-*.csv",
            ],
            location=LOCATION,
        )

        post_execute_prepare_lineage(task, {})

        expected_inlets = [
            BigQueryTable(
                project_id=SOURCE_PROJECT_ID, dataset_id=SOURCE_DATASET_ID, table_id=SOURCE_TABLE_ID
            ),
        ]
        self.assertEqual(task.inlets, expected_inlets)

        expected_outlets = [
            GCSEntity(bucket=TARGET_BUCKET_NAME, path="test.csv"),
            GCSEntity(bucket=TARGET_BUCKET_NAME, path="tmp/test-*.csv"),
        ]
        self.assertEqual(task.outlets, expected_outlets)

    @patch(
        "airflow.composer.data_lineage.transfers.google.cloud.bigquery_to_gcs.BigQueryHook",
        autospec=True,
    )
    def test_post_execute_prepare_lineage_create_hook_error(self, mock_hook):
        mock_hook.side_effect = AirflowException

        task = BigQueryToGCSOperator(
            source_project_dataset_table="", destination_cloud_storage_uris=[], task_id=TASK_ID
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(BigQueryHook, "project_id", new_callable=PropertyMock)
    def test_post_execute_prepare_lineage_parsing_tablename_inlet_error(self, mock_project_id):
        mock_project_id.return_value = SOURCE_PROJECT_ID

        task = self.operator(
            task_id=TASK_ID,
            source_project_dataset_table="invalid_name",
            destination_cloud_storage_uris=[
                f"gs://{TARGET_BUCKET_NAME}/test.csv",
            ],
            location=LOCATION,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(BigQueryHook, "project_id", new_callable=PropertyMock)
    def test_post_execute_prepare_lineage_parsing_uri_outlet_error(self, mock_project_id):
        mock_project_id.return_value = SOURCE_PROJECT_ID

        task = self.operator(
            task_id=TASK_ID,
            source_project_dataset_table=f"{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.{SOURCE_TABLE_ID}",
            destination_cloud_storage_uris=["test.csv"],
            location=LOCATION,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])
