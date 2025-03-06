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

from importlib import reload
from unittest import mock

import pytest
from sqllineage.core.models import Table

from airflow.composer.data_lineage.entities import BigQueryTable
from airflow.composer.data_lineage.utils import (
    _build_BigQueryTable,
    exclude_bigquery_partition,
    exclude_outlet,
    generate_uuid_from_string,
    get_process_id,
    get_run_id,
    is_big_query_table_in_sources,
    parsed_sql_statements,
)


class TestUtils:
    def test_generate_uuid_from_string(self):
        assert generate_uuid_from_string("test-string") == "661f8009-fa8e-56a9-d0e9-4a0a644397d7"

    def test_get_process_id(self):
        assert (
            get_process_id(environment_name="environment-1", dag_id="dag-1", task_id="task-1")
            == "98de46aa-188e-23e0-6a5f-f0f5ed069b08"
        )

    def test_get_run_id(self):
        assert get_run_id(task_instance_run_id="test-run-id") == "570e6350-1fd1-f8f1-e8cf-4b4d1976a8ea"

    @mock.patch.dict("os.environ", {"GCP_PROJECT": "project-1"})
    @mock.patch.dict("os.environ", {"COMPOSER_LOCATION": "us-central1"})
    def test_location_path(self):
        import airflow.composer.data_lineage.utils

        # Reload utils module to reevaluate LOCATION_PATH const with environment variable.
        reload(airflow.composer.data_lineage.utils)

        assert airflow.composer.data_lineage.utils.LOCATION_PATH == "projects/project-1/locations/us-central1"

    @mock.patch("sqlparse.parse")
    @mock.patch("sqlparse.format")
    def test_parsed_sql_statements(self, mock_format, mock_parse):
        sql = "SELECT * FROM test_table"
        mock_format_result = mock.MagicMock()
        mock_format.return_value = mock_format_result
        mock_parse_result = mock.MagicMock()
        skipped_statement = mock.MagicMock(token_first=mock.MagicMock(return_value=False))
        mock_parse.return_value = [skipped_statement, mock_parse_result]

        statements = parsed_sql_statements(sql)

        mock_format.assert_called_with(sql, encoding=None, strip_comments=True)
        mock_parse.assert_called_with(mock_format_result, encoding=None)
        assert statements == [mock_parse_result]

    @pytest.mark.parametrize(
        "source_table, default_dataset, default_project, expected_table",
        [
            (
                Table("test-project.test-dataset.test-table"),
                "default-dataset",
                "default-project",
                BigQueryTable(table_id="test-table", dataset_id="test-dataset", project_id="test-project"),
            ),
            (
                Table("test-project.test-dataset.test-table"),
                None,
                None,
                BigQueryTable(table_id="test-table", dataset_id="test-dataset", project_id="test-project"),
            ),
            (
                Table("test-dataset.test-table"),
                "default-dataset",
                "default-project",
                BigQueryTable(table_id="test-table", dataset_id="test-dataset", project_id="default-project"),
            ),
            (
                Table("test-table"),
                "default-dataset",
                "default-project",
                BigQueryTable(
                    table_id="test-table", dataset_id="default-dataset", project_id="default-project"
                ),
            ),
        ],
    )
    def test_build_table_reference(self, source_table, default_dataset, default_project, expected_table):
        assert _build_BigQueryTable(source_table, default_dataset, default_project) == expected_table

    @pytest.mark.parametrize(
        "query, outlet, default_dataset, default_project, expected_return",
        [
            (
                (
                    "INSERT INTO `project1.dataset1.table1`(a, b) "
                    "select table1.a, table2.b from table1, table2;"
                ),
                BigQueryTable(table_id="table1", dataset_id="dataset1", project_id="project1"),
                "default-dataset",
                "default-project",
                False,
            ),
            (
                (
                    "INSERT INTO `default-project.default-dataset.table1`(a, b) "
                    "select table1.a, table2.b from table1, table2;"
                ),
                BigQueryTable(table_id="table1", dataset_id="default-dataset", project_id="default-project"),
                "default-dataset",
                "default-project",
                True,
            ),
            (
                (
                    "INSERT INTO `project1.dataset1.table1`(a, b) "
                    "select table1.a, table2.b from `project1.dataset1.table1`, table2;"
                ),
                BigQueryTable(table_id="table1", dataset_id="dataset1", project_id="project1"),
                "default-dataset",
                "default-project",
                True,
            ),
            (
                (
                    "INSERT INTO `project1.dataset1.table1`(a, b) "
                    "select table1.a, table2.b from `project1.dataset1.table2`, table1;"
                ),
                BigQueryTable(table_id="table1", dataset_id="dataset1", project_id="project1"),
                None,
                "default-project",
                False,
            ),
            (
                (
                    "INSERT INTO `project2.dataset1.table1`(a, b) "
                    "select table1.a, table2.b from `dataset1.table1`, table2;"
                ),
                BigQueryTable(table_id="table1", dataset_id="dataset1", project_id="project2"),
                "default-dataset",
                "project2",
                True,
            ),
        ],
    )
    def test_is_big_query_table_in_sources(
        self, query, outlet, default_dataset, default_project, expected_return
    ):
        assert (
            is_big_query_table_in_sources(query, outlet, default_dataset, default_project) == expected_return
        )

    def test_exclude_outlet(self):
        n = 10
        dataset_id = "dataset"
        project_id = "project"
        inlets = [
            BigQueryTable(table_id=str(i), dataset_id=dataset_id, project_id=project_id) for i in range(n)
        ]
        outlet = inlets[3]

        result = exclude_outlet(inlets=inlets, outlet=outlet)

        assert result == inlets[:3] + inlets[4:]

    @pytest.mark.parametrize(
        "table_id, expected",
        [
            ("test_table", "test_table"),
            ("test_table$partition", "test_table"),
        ],
    )
    def test_exclude_bigquery_partition(self, table_id, expected):
        actual = exclude_bigquery_partition(table_id=table_id)
        assert actual == expected
