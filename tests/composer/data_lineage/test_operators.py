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

from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.operators.python import PythonOperator


class TestOperators(unittest.TestCase):
    """Tests for airflow.composer.data_lineage.operators.__init__ module.

    Regular path for post_execute_prepare_lineage method is tested for each operator separately.
    """

    def test_post_execute_prepare_lineage_unknown_operator(self):
        # We will never be able to define automated way of preparing lineage metadata for
        # PythonOperator, so let's use it as an unknown operator here.
        task = PythonOperator(
            task_id="test-task",
            python_callable=lambda x: x,
        )

        # Test that post_execute_prepare_lineage doesn't fail for an unknown operator.
        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])
