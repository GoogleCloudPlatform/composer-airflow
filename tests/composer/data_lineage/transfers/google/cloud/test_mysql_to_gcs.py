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
from unittest.mock import patch

from parameterized import parameterized_class

from airflow import AirflowException
from airflow.composer.data_lineage.entities import GCSEntity, MySQLTable
from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

BUCKET = 'test_bucket'
FILENAME = 'test_file'
HOST = 'test-host'
PORT = '1234'
DEFAULT_PORT = '3306'
SCHEMA = 'test_schema'

TEST_TABLE = 'test_table'

SIMPLE_SQL_TEST_TABLE_AS_SOURCE = f'SELECT * FROM {TEST_TABLE}'

WITH_SQL_TEST_TABLE_AS_SOURCE = f'''
WITH avg_students AS
  (SELECT district_id,
          AVG(students) AS average_students
   FROM {TEST_TABLE}
   GROUP BY district_id)
SELECT s.school_name,
       s.district_id,
       avg.average_students
FROM {TEST_TABLE} s
JOIN avg_students AVG ON s.district_id = avg.district_id;'''

SOURCE_TABLE_1 = 'test_table_1'
SOURCE_TABLE_2 = 'test_table_2'
JOIN_SQL_TEST_TABLES_AS_SOURCES = f'''
SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
FROM {SOURCE_TABLE_1}
INNER JOIN {SOURCE_TABLE_2} ON Orders.CustomerID=Customers.CustomerID;
'''

SQL_WITHOUT_SOURCES = '''INSERT INTO TestTable (col1, col2) VALUES ('x', 'y');'''
SQL_ERROR = '''INSERT INTO Table (col1, col2) VALUES ('x', 'y');'''


@parameterized_class(
    [
        {'operator': MySQLToGCSOperator},
        {'operator': MySqlToGoogleCloudStorageOperator},
    ]
)
class TestMySQLToGCSOperator(unittest.TestCase):
    @patch.object(MySqlHook, 'get_uri')
    def test_post_execute_prepare_lineage_sql_simple(self, uri_mock):
        uri_mock.return_value = f'mysql://user:***@{HOST}:{PORT}/{SCHEMA}'

        task = self.operator(
            task_id='test-task', sql=SIMPLE_SQL_TEST_TABLE_AS_SOURCE, bucket=BUCKET, filename=FILENAME
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [MySQLTable(host=HOST, port=PORT, schema=SCHEMA, table=TEST_TABLE)])
        self.assertEqual(task.outlets, [GCSEntity(bucket=BUCKET, path=FILENAME)])

    @patch.object(MySqlHook, 'get_uri')
    def test_post_execute_prepare_lineage_sql_with(self, uri_mock):
        uri_mock.return_value = f'mysql://user:***@{HOST}:{PORT}/{SCHEMA}'

        task = self.operator(
            task_id='test-task', sql=WITH_SQL_TEST_TABLE_AS_SOURCE, bucket=BUCKET, filename=FILENAME
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [MySQLTable(host=HOST, port=PORT, schema=SCHEMA, table=TEST_TABLE)])
        self.assertEqual(task.outlets, [GCSEntity(bucket=BUCKET, path=FILENAME)])

    @patch.object(MySqlHook, 'get_uri')
    def test_post_execute_prepare_lineage_sql_joins(self, uri_mock):
        uri_mock.return_value = f'mysql://user:***@{HOST}:{PORT}/{SCHEMA}'

        task = self.operator(
            task_id='test-task', sql=JOIN_SQL_TEST_TABLES_AS_SOURCES, bucket=BUCKET, filename=FILENAME
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(
            task.inlets,
            [
                MySQLTable(host=HOST, port=PORT, schema=SCHEMA, table=_t)
                for _t in [SOURCE_TABLE_1, SOURCE_TABLE_2]
            ],
        )
        self.assertEqual(task.outlets, [GCSEntity(bucket=BUCKET, path=FILENAME)])

    @patch.object(MySqlHook, 'get_uri')
    def test_post_execute_prepare_lineage_default_port(self, uri_mock):
        uri_mock.return_value = f'mysql://user:***@{HOST}/{SCHEMA}'

        task = self.operator(
            task_id='test-task', sql=SIMPLE_SQL_TEST_TABLE_AS_SOURCE, bucket=BUCKET, filename=FILENAME
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(
            task.inlets, [MySQLTable(host=HOST, port=DEFAULT_PORT, schema=SCHEMA, table=TEST_TABLE)]
        )
        self.assertEqual(task.outlets, [GCSEntity(bucket=BUCKET, path=FILENAME)])

    @patch.object(MySqlHook, 'get_uri')
    def test_post_execute_prepare_lineage_no_sources(self, uri_mock):
        uri_mock.return_value = f'mysql://user:***@{HOST}:{PORT}/{SCHEMA}'

        task = self.operator(task_id='test-task', sql=SQL_WITHOUT_SOURCES, bucket=BUCKET, filename=FILENAME)

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch('airflow.composer.data_lineage.transfers.google.cloud.mysql_to_gcs.MySqlHook', autospec=True)
    def test_post_execute_prepare_lineage_hook_creation_error(self, hook_mock):
        hook_mock.side_effect = AirflowException

        task = self.operator(
            task_id='test-task', sql=SIMPLE_SQL_TEST_TABLE_AS_SOURCE, bucket=BUCKET, filename=FILENAME
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(MySqlHook, 'get_uri')
    def test_post_execute_prepare_lineage_connection_error(self, uri_mock):
        uri_mock.side_effect = AirflowNotFoundException

        task = self.operator(
            task_id='test-task', sql=SIMPLE_SQL_TEST_TABLE_AS_SOURCE, bucket=BUCKET, filename=FILENAME
        )

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])

    @patch.object(MySqlHook, 'get_uri')
    def test_post_execute_prepare_lineage_sql_parsing_error(self, uri_mock):
        uri_mock.return_value = f'mysql://user:***@{HOST}:{PORT}/{SCHEMA}'

        task = self.operator(task_id='test-task', sql=SQL_ERROR, bucket=BUCKET, filename=FILENAME)

        post_execute_prepare_lineage(task, {})

        self.assertEqual(task.inlets, [])
        self.assertEqual(task.outlets, [])
