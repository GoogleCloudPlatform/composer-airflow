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

from parameterized import parameterized, parameterized_class

from airflow.composer.data_lineage.entities import GCSEntity
from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


@parameterized_class(
    [
        {'operator': GCSToGCSOperator},
        {'operator': GoogleCloudStorageToGoogleCloudStorageOperator},
    ]
)
class TestGCSToGCSOperator(unittest.TestCase):
    @parameterized.expand(
        [
            (
                'file_new_name_old_bucket',
                'file_test.txt',
                None,
                'new_name.txt',
                [GCSEntity(bucket='source_bucket', path='file_test.txt')],
                [GCSEntity(bucket='source_bucket', path='new_name.txt')],
            ),
            (
                'file_new_name_new_bucket',
                'file_test.txt',
                'target_bucket',
                'new_name.txt',
                [GCSEntity(bucket='source_bucket', path='file_test.txt')],
                [GCSEntity(bucket='target_bucket', path='new_name.txt')],
            ),
            (
                'file_new_name_path_old_bucket',
                'file_test.txt',
                None,
                'folder/new_name.txt',
                [GCSEntity(bucket='source_bucket', path='file_test.txt')],
                [GCSEntity(bucket='source_bucket', path='folder/new_name.txt')],
            ),
            (
                'file_new_name_path_new_bucket',
                'file_test.txt',
                'target_bucket',
                'folder/new_name.txt',
                [GCSEntity(bucket='source_bucket', path='file_test.txt')],
                [GCSEntity(bucket='target_bucket', path='folder/new_name.txt')],
            ),
            (
                'folder_new_name_old_bucket',
                'old_folder/',
                None,
                'new_folder/',
                [GCSEntity(bucket='source_bucket', path='old_folder/')],
                [GCSEntity(bucket='source_bucket', path='new_folder/')],
            ),
            (
                'folder_new_name_new_bucket',
                'old_folder/',
                'target_bucket',
                'new_folder/',
                [GCSEntity(bucket='source_bucket', path='old_folder/')],
                [GCSEntity(bucket='target_bucket', path='new_folder/')],
            ),
            (
                'folder_new_name_path_old_bucket',
                'old_folder/',
                None,
                'new_home/new_folder/',
                [GCSEntity(bucket='source_bucket', path='old_folder/')],
                [GCSEntity(bucket='source_bucket', path='new_home/new_folder/')],
            ),
            (
                'folder_new_name_path_new_bucket',
                'old_folder/',
                'target_bucket',
                'new_home/new_folder/',
                [GCSEntity(bucket='source_bucket', path='old_folder/')],
                [GCSEntity(bucket='target_bucket', path='new_home/new_folder/')],
            ),
            (
                'file_old_name_new_bucket',
                'file_test.txt',
                'target_bucket',
                None,
                [GCSEntity(bucket='source_bucket', path='file_test.txt')],
                [GCSEntity(bucket='target_bucket', path='file_test.txt')],
            ),
            (
                'file_old_name_old_path_new_bucket',
                'old/path/file_test.txt',
                'target_bucket',
                None,
                [GCSEntity(bucket='source_bucket', path='old/path/file_test.txt')],
                [GCSEntity(bucket='target_bucket', path='old/path/file_test.txt')],
            ),
            (
                'folder_old_name_new_bucket',
                'old_folder/',
                'target_bucket',
                None,
                [GCSEntity(bucket='source_bucket', path='old_folder/')],
                [GCSEntity(bucket='target_bucket', path='old_folder/')],
            ),
            (
                'folder_old_name_old_path_new_bucket',
                'old/path/old_folder/',
                'target_bucket',
                None,
                [GCSEntity(bucket='source_bucket', path='old/path/old_folder/')],
                [GCSEntity(bucket='target_bucket', path='old/path/old_folder/')],
            ),
            (
                'files_wildcard_old_name_old_bucket',
                'old/path/*.csv',
                None,
                None,
                [GCSEntity(bucket='source_bucket', path='old/path/*.csv')],
                [GCSEntity(bucket='source_bucket', path='old/path/*.csv')],
            ),
            (
                'files_wildcard_old_name_new_bucket',
                'old/path/*.csv',
                'target_bucket',
                None,
                [GCSEntity(bucket='source_bucket', path='old/path/*.csv')],
                [GCSEntity(bucket='target_bucket', path='old/path/*.csv')],
            ),
            (
                'files_wildcard_new_name_old_bucket',
                'old/path/*.csv',
                None,
                'new/path/',
                [GCSEntity(bucket='source_bucket', path='old/path/*.csv')],
                [GCSEntity(bucket='source_bucket', path='new/path/')],
            ),
            (
                'files_wildcard_new_name_new_bucket',
                'old/path/*.csv',
                'target_bucket',
                'new/path/',
                [GCSEntity(bucket='source_bucket', path='old/path/*.csv')],
                [GCSEntity(bucket='target_bucket', path='new/path/')],
            ),
        ]
    )
    def test_post_execute_prepare_lineage_single_object(
        self, _, source_object, destination_bucket, destination_object, expected_inlets, expected_outlets
    ):
        task = self.operator(
            task_id='test-task',
            source_bucket='source_bucket',
            source_object=source_object,
            destination_bucket=destination_bucket,
            destination_object=destination_object,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, expected_inlets)

        self.assertEqual(task.outlets, expected_outlets)

    @parameterized.expand(
        [
            (
                'files_old_path_new_bucket',
                ['file_1.txt', 'file_2.txt'],
                'target_bucket',
                None,
                [
                    GCSEntity(bucket='source_bucket', path='file_1.txt'),
                    GCSEntity(bucket='source_bucket', path='file_2.txt'),
                ],
                [
                    GCSEntity(bucket='target_bucket', path='file_1.txt'),
                    GCSEntity(bucket='target_bucket', path='file_2.txt'),
                ],
            ),
            (
                'folders_old_path_new_bucket',
                ['folder_1/', 'folder_2/'],
                'target_bucket',
                None,
                [
                    GCSEntity(bucket='source_bucket', path='folder_1/'),
                    GCSEntity(bucket='source_bucket', path='folder_2/'),
                ],
                [
                    GCSEntity(bucket='target_bucket', path='folder_1/'),
                    GCSEntity(bucket='target_bucket', path='folder_2/'),
                ],
            ),
            (
                'files_new_path_new_bucket',
                ['old/path/file_1.txt', 'old/path/file_2.txt'],
                'target_bucket',
                'new/path/',
                [
                    GCSEntity(bucket='source_bucket', path='old/path/file_1.txt'),
                    GCSEntity(bucket='source_bucket', path='old/path/file_2.txt'),
                ],
                [GCSEntity(bucket='target_bucket', path='new/path/')],
            ),
            (
                'folders_new_path_new_bucket',
                ['old/path/1/', 'old/path/2/'],
                'target_bucket',
                'new/path/',
                [
                    GCSEntity(bucket='source_bucket', path='old/path/1/'),
                    GCSEntity(bucket='source_bucket', path='old/path/2/'),
                ],
                [GCSEntity(bucket='target_bucket', path='new/path/')],
            ),
            (
                'folders_new_path_old_bucket',
                ['old/path/1/', 'old/path/2/'],
                None,
                'new/path/',
                [
                    GCSEntity(bucket='source_bucket', path='old/path/1/'),
                    GCSEntity(bucket='source_bucket', path='old/path/2/'),
                ],
                [GCSEntity(bucket='source_bucket', path='new/path/')],
            ),
            (
                'files_new_path_old_bucket',
                ['old/path/file_1.txt', 'old/path/file_2.txt'],
                None,
                'new/path/',
                [
                    GCSEntity(bucket='source_bucket', path='old/path/file_1.txt'),
                    GCSEntity(bucket='source_bucket', path='old/path/file_2.txt'),
                ],
                [GCSEntity(bucket='source_bucket', path='new/path/')],
            ),
            (
                'files_wildcard_old_name_old_bucket',
                ['old/path/1/*.csv', 'old/path/2/*.csv'],
                None,
                None,
                [
                    GCSEntity(bucket='source_bucket', path='old/path/1/*.csv'),
                    GCSEntity(bucket='source_bucket', path='old/path/2/*.csv'),
                ],
                [
                    GCSEntity(bucket='source_bucket', path='old/path/1/*.csv'),
                    GCSEntity(bucket='source_bucket', path='old/path/2/*.csv'),
                ],
            ),
            (
                'files_wildcard_old_name_new_bucket',
                ['old/path/1/*.csv', 'old/path/2/*.csv'],
                'target_bucket',
                None,
                [
                    GCSEntity(bucket='source_bucket', path='old/path/1/*.csv'),
                    GCSEntity(bucket='source_bucket', path='old/path/2/*.csv'),
                ],
                [
                    GCSEntity(bucket='target_bucket', path='old/path/1/*.csv'),
                    GCSEntity(bucket='target_bucket', path='old/path/2/*.csv'),
                ],
            ),
            (
                'files_wildcard_new_name_old_bucket',
                ['old/path/1/*.csv', 'old/path/2/*.csv'],
                None,
                'new/path/',
                [
                    GCSEntity(bucket='source_bucket', path='old/path/1/*.csv'),
                    GCSEntity(bucket='source_bucket', path='old/path/2/*.csv'),
                ],
                [GCSEntity(bucket='source_bucket', path='new/path/')],
            ),
            (
                'files_wildcard_new_name_new_bucket',
                ['old/path/1/*.csv', 'old/path/2/*.csv'],
                'target_bucket',
                'new/path/',
                [
                    GCSEntity(bucket='source_bucket', path='old/path/1/*.csv'),
                    GCSEntity(bucket='source_bucket', path='old/path/2/*.csv'),
                ],
                [GCSEntity(bucket='target_bucket', path='new/path/')],
            ),
        ]
    )
    def test_post_execute_prepare_lineage_multiple_objects(
        self, _, source_objects, destination_bucket, destination_object, expected_inlets, expected_outlets
    ):
        task = self.operator(
            task_id='test-task',
            source_bucket='source_bucket',
            source_objects=source_objects,
            destination_bucket=destination_bucket,
            destination_object=destination_object,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, expected_inlets)

        self.assertEqual(task.outlets, expected_outlets)
