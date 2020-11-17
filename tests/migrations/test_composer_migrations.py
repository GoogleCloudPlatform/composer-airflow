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
"""Tests for Composer migrations."""

import unittest

from parameterized import parameterized
from sqlalchemy.engine.reflection import Inspector

from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session


class TestComposerMigrations(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from airflow import settings

        settings.configure_orm()

    @parameterized.expand(
        [
            ['connection', 'host', 100],
            ['job', 'hostname', 100],
            ['task_instance', 'hostname', 100],
        ]
    )
    def test_hostname_columns_adjustments(self, table, column_name, length):
        with create_session() as session:
            inspector = Inspector.from_engine(session.connection())
            columns = inspector.get_columns(table)
            for column in columns:
                if column['name'] == column_name:
                    self.assertEqual(
                        column['type'].length,
                        length,
                        f'Length of {column_name} for {table} is not expected',
                    )
                    break
            else:
                self.fail(f'{column_name} column not found for {table}')

    def test_task_instance_index(self):
        with create_session() as session:
            inspector = Inspector.from_engine(session.connection())
            indices = inspector.get_indexes('task_instance')
            self.assertTrue(any(index['name'] == 'ti_worker_healthcheck' for index in indices))

        self.assertTrue(any(index.name == 'ti_worker_healthcheck' for index in TaskInstance.__table_args__))
