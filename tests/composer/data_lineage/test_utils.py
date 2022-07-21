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
from importlib import reload
from unittest import mock

from airflow.composer.data_lineage.entities import BigQueryTable
from airflow.composer.data_lineage.utils import (
    exclude_outlet,
    generate_uuid_from_string,
    get_process_id,
    get_run_id,
)


class TestUtils(unittest.TestCase):
    def test_generate_uuid_from_string(self):
        self.assertEqual(
            generate_uuid_from_string("test-string"),
            "661f8009-fa8e-56a9-d0e9-4a0a644397d7",
        )

    def test_get_process_id(self):
        self.assertEqual(
            get_process_id(environment_name="environment-1", dag_id="dag-1", task_id="task-1"),
            "98de46aa-188e-23e0-6a5f-f0f5ed069b08",
        )

    def test_get_run_id(self):
        self.assertEqual(
            get_run_id(task_instance_run_id="test-run-id"),
            "570e6350-1fd1-f8f1-e8cf-4b4d1976a8ea",
        )

    @mock.patch.dict("os.environ", {"GCP_PROJECT": "project-1"})
    @mock.patch.dict("os.environ", {"COMPOSER_LOCATION": "us-central1"})
    def test_location_path(self):
        import airflow.composer.data_lineage.utils

        # Reload utils module to reevaluate LOCATION_PATH const with environment variable.
        reload(airflow.composer.data_lineage.utils)

        self.assertEqual(
            airflow.composer.data_lineage.utils.LOCATION_PATH,
            "projects/project-1/locations/us-central1",
        )

    def test_exclude_outlet(self):
        n = 4
        dataset_id = "dataset"
        project_id = "project"
        inlets = [
            BigQueryTable(table_id=str(i), dataset_id=dataset_id, project_id=project_id) for i in range(n)
        ]
        outlet = inlets[0]

        result = exclude_outlet(inlets=inlets, outlet=outlet)

        self.assertEqual(result, inlets[1:])
