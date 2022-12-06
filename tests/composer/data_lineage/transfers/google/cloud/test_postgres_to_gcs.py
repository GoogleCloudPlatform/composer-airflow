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
from unittest.mock import patch

from parameterized import parameterized, parameterized_class

from airflow import AirflowException
from airflow.composer.data_lineage.entities import GCSEntity, PostgresTable
from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

TASK_ID = "test-postgres-to-gcs"
POSTGRES_CONN_ID = "postgres_default"
TARGET_BUCKET_NAME = "bucket_name"
TARGET_BUCKET_FILENAME = "test.csv"
SOURCE_PROJECT_ID = "source_project"
HOST = "test_host"
PORT = "1111"
SCHEMA = "test_schema"
DATABASE = "test_db"
SQL_MULTIPLE_QUERIES_WITHOUT_SOURCES = '''
INSERT INTO test_target_table_1 (column_1, column_2, OWNER)
VALUES ( 'value 1', 'value 2', 'Jane');
INSERT INTO test_target_table_2 (column_1, column_2, OWNER)
VALUES ( 'value 1', 'value 2', 'Jane');
'''
SQL_QUERY_WITHOUT_SELECT_STATEMENT = '''
INSERT INTO A SELECT * FROM B
'''


@parameterized_class(
    [
        {"operator": PostgresToGCSOperator},
        {"operator": PostgresToGoogleCloudStorageOperator},
    ]
)
class TestPostgresToGCSOperator(unittest.TestCase):
    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_single_source(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = self.operator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="SELECT * FROM source_table;",
            bucket=TARGET_BUCKET_NAME,
            filename=TARGET_BUCKET_FILENAME,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(
            task.inlets,
            [PostgresTable(host=HOST, port=PORT, database=DATABASE, schema="public", table="source_table")],
        )
        self.assertEqual(task.outlets, [GCSEntity(bucket=TARGET_BUCKET_NAME, path=TARGET_BUCKET_FILENAME)])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_with_subquery(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = self.operator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=(
                "SELECT test_column_1, test_column_2 FROM source_table WHERE test_column_3 IN "
                "(SELECT test_column_3 FROM source_table_2 WHERE test_column_4 = 'test')"
            ),
            bucket=TARGET_BUCKET_NAME,
            filename=TARGET_BUCKET_FILENAME,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(
            task.inlets,
            [
                PostgresTable(host=HOST, port=PORT, database=DATABASE, schema="public", table="source_table"),
                PostgresTable(
                    host=HOST, port=PORT, database=DATABASE, schema="public", table="source_table_2"
                ),
            ],
        )
        self.assertEqual(task.outlets, [GCSEntity(bucket=TARGET_BUCKET_NAME, path=TARGET_BUCKET_FILENAME)])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_with_db_schema(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = self.operator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="SELECT * FROM source_test_db.source_test_schema.source_table;",
            bucket=TARGET_BUCKET_NAME,
            filename=TARGET_BUCKET_FILENAME,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(
            task.inlets,
            [
                PostgresTable(
                    host=HOST,
                    port=PORT,
                    database="source_test_db",
                    schema="source_test_schema",
                    table="source_table",
                )
            ],
        )
        self.assertEqual(task.outlets, [GCSEntity(bucket=TARGET_BUCKET_NAME, path=TARGET_BUCKET_FILENAME)])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_with_schema(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = self.operator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="SELECT * FROM source_test_schema.source_table;",
            bucket=TARGET_BUCKET_NAME,
            filename=TARGET_BUCKET_FILENAME,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(
            task.inlets,
            [
                PostgresTable(
                    host=HOST,
                    port=PORT,
                    database=DATABASE,
                    schema="source_test_schema",
                    table="source_table",
                )
            ],
        )
        self.assertEqual(task.outlets, [GCSEntity(bucket=TARGET_BUCKET_NAME, path=TARGET_BUCKET_FILENAME)])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_default_port(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}/{DATABASE}"

        task = self.operator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="SELECT * FROM source_table;",
            bucket=TARGET_BUCKET_NAME,
            filename=TARGET_BUCKET_FILENAME,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(
            task.inlets,
            [PostgresTable(host=HOST, port="5432", database=DATABASE, schema="public", table="source_table")],
        )
        self.assertEqual(task.outlets, [GCSEntity(bucket=TARGET_BUCKET_NAME, path=TARGET_BUCKET_FILENAME)])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_no_source_tables_error(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = self.operator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="CREATE SCHEMA source_test_schema;",
            bucket=TARGET_BUCKET_NAME,
            filename=TARGET_BUCKET_FILENAME,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_parsing_db_schema_parts_error(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = self.operator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="SELECT * FROM test.test_db.test_schema.test.source_table;",
            bucket=TARGET_BUCKET_NAME,
            filename=TARGET_BUCKET_FILENAME,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch(
        "airflow.composer.data_lineage.transfers.google.cloud.postgres_to_gcs.PostgresHook",
        autospec=True,
    )
    def test_post_execute_prepare_lineage_create_hook_error(self, mock_hook):
        mock_hook.side_effect = AirflowException

        task = self.operator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="SELECT * FROM source_table;",
            bucket=TARGET_BUCKET_NAME,
            filename=TARGET_BUCKET_FILENAME,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_parsing_incorrect_query_error(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = self.operator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="""SELECT * FROM WHERE test='test'""",
            bucket=TARGET_BUCKET_NAME,
            filename=TARGET_BUCKET_FILENAME,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_connection_error(self, mock_get_uri):
        mock_get_uri.side_effect = AirflowNotFoundException

        task = self.operator(
            task_id="test-task",
            sql="SELECT * FROM source_table;",
            bucket=TARGET_BUCKET_NAME,
            filename=TARGET_BUCKET_FILENAME,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sqlparse_type_error(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = self.operator(
            task_id="test-task", sql=None, bucket=TARGET_BUCKET_NAME, filename=TARGET_BUCKET_FILENAME
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_multiple_queries(self, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"

        task = self.operator(
            task_id='test-task',
            sql='''
            INSERT INTO test_target_table (column_1, column_2, OWNER)
            VALUES ( 'value 1', 'value 2', 'Jane');
            SELECT * FROM source_table_1;
            SELECT * FROM source_table_2;
            ''',
            bucket=TARGET_BUCKET_NAME,
            filename=TARGET_BUCKET_FILENAME,
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(
            task.inlets,
            [PostgresTable(host=HOST, port=PORT, database=DATABASE, schema="public", table="source_table_1")],
        )
        self.assertEqual(task.outlets, [GCSEntity(bucket=TARGET_BUCKET_NAME, path=TARGET_BUCKET_FILENAME)])

    @parameterized.expand(
        [
            ("multiple_queries_without_sources", SQL_MULTIPLE_QUERIES_WITHOUT_SOURCES),
            ("multiple_queries_without_select", SQL_QUERY_WITHOUT_SELECT_STATEMENT),
        ]
    )
    @patch.object(PostgresHook, "get_uri")
    def test_post_execute_prepare_lineage_sql_no_select(self, _, sql, mock_get_uri):
        mock_get_uri.return_value = f"postgres://login:password@{HOST}:{PORT}/{DATABASE}"
        task = self.operator(
            task_id="test-task", sql=sql, bucket=TARGET_BUCKET_NAME, filename=TARGET_BUCKET_FILENAME
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])
