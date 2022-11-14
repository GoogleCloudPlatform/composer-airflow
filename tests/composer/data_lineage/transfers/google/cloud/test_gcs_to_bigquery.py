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
from unittest.mock import PropertyMock, patch

from parameterized import parameterized, parameterized_class

from airflow import AirflowException
from airflow.composer.data_lineage.entities import BigQueryTable, GCSEntity
from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

BUCKET = 'test_bucket'
PROJECT_ID = 'test_project_id'
DATASET_ID = 'test_dataset_id'
TABLE_ID = 'test_table_id'


@parameterized_class(
    [
        {'operator': GCSToBigQueryOperator},
        {'operator': GoogleCloudStorageToBigQueryOperator},
    ]
)
class TestGCSToBigQuery(unittest.TestCase):
    @parameterized.expand(
        [
            (
                'copy_one_file_short_destination',
                ['path/to/file.csv'],
                f'{DATASET_ID}.{TABLE_ID}',
                [GCSEntity(bucket=BUCKET, path='path/to/file.csv')],
                [BigQueryTable(project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID)],
            ),
            (
                'copy_one_file_full_destination_1',
                ['path/to/file.csv'],
                f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}',
                [GCSEntity(bucket=BUCKET, path='path/to/file.csv')],
                [BigQueryTable(project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID)],
            ),
            (
                'copy_one_file_full_destination_2',
                ['path/to/file.csv'],
                f'{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}',
                [GCSEntity(bucket=BUCKET, path='path/to/file.csv')],
                [BigQueryTable(project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID)],
            ),
            (
                'copy_multiple_files_short_destination',
                ['path/to/file_1.csv', 'path/to/file_2.csv'],
                f'{DATASET_ID}.{TABLE_ID}',
                [
                    GCSEntity(bucket=BUCKET, path='path/to/file_1.csv'),
                    GCSEntity(bucket=BUCKET, path='path/to/file_2.csv'),
                ],
                [BigQueryTable(project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID)],
            ),
            (
                'copy_multiple_files_full_destination_1',
                ['path/to/file_1.csv', 'path/to/file_2.csv'],
                f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}',
                [
                    GCSEntity(bucket=BUCKET, path='path/to/file_1.csv'),
                    GCSEntity(bucket=BUCKET, path='path/to/file_2.csv'),
                ],
                [BigQueryTable(project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID)],
            ),
            (
                'copy_multiple_files_full_destination_2',
                ['path/to/file_1.csv', 'path/to/file_2.csv'],
                f'{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}',
                [
                    GCSEntity(bucket=BUCKET, path='path/to/file_1.csv'),
                    GCSEntity(bucket=BUCKET, path='path/to/file_2.csv'),
                ],
                [BigQueryTable(project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID)],
            ),
        ]
    )
    @patch.object(BigQueryHook, 'project_id', new_callable=PropertyMock)
    def test_post_execute_prepare_lineage(
        self, _, source_objects, destination, expected_inlets, expected_outlets, mock_hook
    ):
        mock_hook.return_value = PROJECT_ID

        task = self.operator(
            task_id='test_task',
            bucket=BUCKET,
            source_objects=source_objects,
            destination_project_dataset_table=destination,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, expected_inlets)
        self.assertEqual(task.outlets, expected_outlets)

    @patch('airflow.composer.data_lineage.transfers.google.cloud.gcs_to_bigquery.BigQueryHook', autospec=True)
    def test_post_execute_prepare_lineage_big_query_hook_error(self, mock_hook):
        mock_hook.side_effect = AirflowException

        task = self.operator(
            task_id='test_task',
            bucket=BUCKET,
            source_objects=['test_path'],
            destination_project_dataset_table=f'{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}',
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(BigQueryHook, 'project_id', new_callable=PropertyMock)
    def test_post_execute_prepare_lineage_split_table_name_error(self, mock_hook):
        mock_hook.project_id = PROJECT_ID

        task = self.operator(
            task_id='test_task',
            bucket=BUCKET,
            source_objects=['test_path'],
            destination_project_dataset_table='wrong_destination',
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])
