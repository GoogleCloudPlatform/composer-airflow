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
from unittest.mock import PropertyMock, patch

from airflow import AirflowException
from airflow.composer.data_lineage.entities import BigQueryTable
from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.composer.data_lineage.transfers.google.cloud.bigquery_to_bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator


class TestBigQueryToBigQuery(unittest.TestCase):
    @patch.object(BigQueryHook, "project_id", new_callable=PropertyMock)
    def test_post_execute_prepare_lineage_single_source(self, mock_project_id):
        source = "source_project.source_dataset.source_table"
        target = "target_project.target_dataset.target_table"

        mock_project_id.return_value = "source_project"

        task = BigQueryToBigQueryOperator(
            source_project_dataset_tables=source,
            destination_project_dataset_table=target,
            task_id="test-task",
        )

        post_execute_prepare_lineage(task, {})

        expected_inlets = [
            BigQueryTable(project_id="source_project", dataset_id="source_dataset", table_id="source_table"),
        ]
        self.assertEqual(task.inlets, expected_inlets)

        expected_outlets = [
            BigQueryTable(project_id="target_project", dataset_id="target_dataset", table_id="target_table"),
        ]
        self.assertEqual(task.outlets, expected_outlets)

    @patch.object(BigQueryHook, "project_id", new_callable=PropertyMock)
    def test_post_execute_prepare_lineage_multiple_sources(self, mock_project_id):
        sources = [
            "src_project1.src_dataset1.src_table1",
            "src_project2:src_dataset2.src_table2",
            "src_dataset3.src_table3",
        ]
        target = "target_project.target_dataset.target_table"

        mock_project_id.return_value = "source_project"

        task = BigQueryToBigQueryOperator(
            source_project_dataset_tables=sources,
            destination_project_dataset_table=target,
            task_id="test-task",
        )

        post_execute_prepare_lineage(task, {})

        expected_inlets = [
            BigQueryTable(project_id="src_project1", dataset_id="src_dataset1", table_id="src_table1"),
            BigQueryTable(project_id="src_project2", dataset_id="src_dataset2", table_id="src_table2"),
            BigQueryTable(project_id="source_project", dataset_id="src_dataset3", table_id="src_table3"),
        ]
        self.assertEqual(task.inlets, expected_inlets)

        expected_outlets = [
            BigQueryTable(project_id="target_project", dataset_id="target_dataset", table_id="target_table")
        ]
        self.assertEqual(task.outlets, expected_outlets)

    @patch(
        "airflow.composer.data_lineage.transfers.google.cloud.bigquery_to_bigquery.BigQueryHook",
        autospec=True,
    )
    def test_post_execute_prepare_lineage_create_hook_error(self, mock_hook):
        mock_hook.side_effect = AirflowException

        task = BigQueryToBigQueryOperator(
            source_project_dataset_tables=[], destination_project_dataset_table="", task_id="test-task"
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(BigQueryHook, "project_id", new_callable=PropertyMock)
    def test_post_execute_prepare_lineage_split_tablenames_inlet_error(self, mock_project_id):
        good_source = "src_project1.src_dataset1.src_table1"
        sources = [
            "incorrect.table.name.1",
            good_source,
        ]
        target = "target_project.target_dataset.target_table"

        mock_project_id.return_value = "source_project1"

        task = BigQueryToBigQueryOperator(
            source_project_dataset_tables=sources,
            destination_project_dataset_table=target,
            task_id="test-task",
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(BigQueryHook, "project_id", new_callable=PropertyMock)
    def test_post_execute_prepare_lineage_split_tablenames_outlet_error(self, mock_project_id):
        sources = ["src_project1.src_dataset1.src_table1", "src_project2.src_dataset2.src_table2"]
        target = "incorrect destination"

        mock_project_id.return_value = "src_project1"

        task = BigQueryToBigQueryOperator(
            source_project_dataset_tables=sources,
            destination_project_dataset_table=target,
            task_id="test-task",
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])
