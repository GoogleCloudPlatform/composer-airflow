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
from __future__ import annotations

import unittest
from unittest.mock import patch

from airflow import AirflowException
from airflow.composer.data_lineage.entities import PostgresTable
from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

TASK_ID = "test-postgres"
POSTGRES_CONN_ID = "postgres_default"
SOURCE_PROJECT_ID = "source_project"
HOST = "test_host"
PORT = "1111"
SCHEMA = "test_schema"
DATABASE = "test_db"


class TestPostgresOperator(unittest.TestCase):
    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_inserts_without_target_table(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=(
                "SELECT * FROM source_table;"
                "INSERT INTO test_table (test_str_1) VALUES ('10');"
                "INSERT INTO test_table_2 (test_str_1) VALUES ('5');"
            ),
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_update_without_target_table(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="UPDATE test_table SET test_column = 'test' Where test_column_1 = 'test_1';",
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_create_table_with_source_target_tables(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="CREATE TABLE target_table AS SELECT * FROM source_table;",
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(
            task.inlets,
            [PostgresTable(host=HOST, port=PORT, database=DATABASE, schema="public", table="source_table")],
        )
        self.assertEqual(
            task.outlets,
            [PostgresTable(host=HOST, port=PORT, database=DATABASE, schema="public", table="target_table")],
        )

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_insert_with_source_target_tables(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="INSERT INTO target_table SELECT * FROM source_table",
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(
            task.inlets,
            [PostgresTable(host=HOST, port=PORT, database=DATABASE, schema="public", table="source_table")],
        )
        self.assertEqual(
            task.outlets,
            [PostgresTable(host=HOST, port=PORT, database=DATABASE, schema="public", table="target_table")],
        )

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_with_sources_target_tables(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id="test-task",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=(
                "CREATE TABLE target_table AS "
                "(SELECT test_column FROM source_table_1 "
                "INNER JOIN source_table_2 ON source_table_1.test_column = source_table_2.test_column);"
            ),
        )

        post_execute_prepare_lineage(task, {})
        self.assertEqual(
            task.inlets,
            [
                PostgresTable(
                    host=HOST, port=PORT, database=DATABASE, schema="public", table="source_table_1"
                ),
                PostgresTable(
                    host=HOST, port=PORT, database=DATABASE, schema="public", table="source_table_2"
                ),
            ],
        )
        self.assertEqual(
            task.outlets,
            [PostgresTable(host=HOST, port=PORT, database=DATABASE, schema="public", table="target_table")],
        )

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_create_db_schema_table_error(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="CREATE TABLE IF NOT EXISTS test.test.test_database.test_schema_new.test_table;",
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_multi_queries_str_with_source_target_tables(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=(
                "CREATE TABLE IF NOT EXISTS test_database.test_schema_1.test_table ("
                "test_num_1 SERIAL PRIMARY KEY;"
                "CREATE TABLE target_table AS SELECT * FROM source_table;"
                "test_str_1 VARCHAR NOT NULL);"
            ),
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_multi_queries_list_source_target_tables(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=[
                "INSERT INTO test_table_1 (test_str_1) VALUES ('10');",
                "CREATE TABLE target_table AS SELECT * FROM source_table;",
                "INSERT INTO test_table_2 (test_str_1) VALUES ('10');"
                "CREATE TABLE target_table_2 AS SELECT * FROM source_table_2;",
            ],
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(
            task.inlets,
            [PostgresTable(host=HOST, port=PORT, database=DATABASE, schema="public", table="source_table")],
        )
        self.assertEqual(
            task.outlets,
            [PostgresTable(host=HOST, port=PORT, database=DATABASE, schema="public", table="target_table")],
        )

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_multi_queries_in_list(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=[
                "INSERT INTO test_table_1 (test_str_1) VALUES ('10'); SELECT * FROM source_table_2;",
                "INSERT INTO test_table_1 (test_str_1) VALUES ('10');"
                "CREATE TABLE target_table AS SELECT * FROM source_table;"
                "INSERT INTO test_table_2 (test_str_1) VALUES ('10');",
            ],
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(
            task.inlets,
            [PostgresTable(host=HOST, port=PORT, database=DATABASE, schema="public", table="source_table")],
        )
        self.assertEqual(
            task.outlets,
            [PostgresTable(host=HOST, port=PORT, database=DATABASE, schema="public", table="target_table")],
        )

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_multi_queries_nested_list_error(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=[
                "INSERT INTO test_table_1 (test_str_1) VALUES ('10');",
                [
                    "CREATE TABLE target_table_2 AS SELECT * FROM source_table_2;",
                ],
                "CREATE TABLE target_table AS SELECT * FROM source_table;",
                "INSERT INTO test_table_2 (test_str_1) VALUES ('10');",
            ],
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_connection_error(self, mock_get_uri):
        mock_get_uri.side_effect = AirflowNotFoundException

        task = PostgresOperator(
            task_id="test-task",
            sql="",
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresOperator, "get_db_hook")
    def test_post_execute_prepare_lineage_create_hook_error(self, mock_get_db_hook):
        mock_get_db_hook.side_effect = AirflowException

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="SELECT * FROM source_table;",
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_parsing_incorrect_query_error(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="SELECT * FROM WHERE test='test';",
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_source_target_split_error(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=(
                "CREATE TABLE test.test_db.test_schema.target_table "
                "AS SELECT * FROM test.test.test_db.test_schema.source_table;"
            ),
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_target_split_error(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=(
                "CREATE TABLE test_schema.target_table "
                "AS SELECT * FROM test.test.test_db.test_schema.source_table;"
            ),
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_none_error(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = PostgresOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=None,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])
