#
# Copyright 2025 Google LLC
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

from unittest import mock

from sqlalchemy import Index
from sqlalchemy.engine.reflection import Inspector

from airflow.composer.patches.database_migrations.monkey_patching.airflow_utils_db import (
    _apply_composer_migrations,
    patch,
)
from airflow.models.taskinstance import TaskInstance
from airflow.utils import db
from airflow.utils.session import provide_session


class TestAirflowUtilsDb:
    @classmethod
    def setup_class(cls):
        patch()

    @mock.patch(
        "airflow.composer.patches.database_migrations.monkey_patching.airflow_utils_db._apply_composer_migrations",
        autospec=True,
    )
    def test_patch(self, apply_composer_migrations_mock):
        db.upgradedb()

        apply_composer_migrations_mock.assert_called()

    @provide_session
    def test_apply_composer_migrations_taskinstance_worker_healthcheck_index(self, session):
        def _index_exists():
            connection = session.get_bind()
            inspector = Inspector.from_engine(connection)

            for ind in inspector.get_indexes("task_instance"):
                if ind["name"] == "ti_worker_healthcheck":
                    return True

            return False

        # Drop index.
        Index(
            "ti_worker_healthcheck",
            TaskInstance.end_date,
            TaskInstance.hostname,
            TaskInstance.state,
            unique=False,
        ).drop(bind=session.get_bind(), checkfirst=True)
        assert not _index_exists()

        _apply_composer_migrations()

        assert _index_exists()

    @provide_session
    def test_apply_composer_migrations_length_of_hostname_columns(self, session):
        def _length_of_column(table, column_name):
            connection = session.get_bind()
            inspector = Inspector.from_engine(connection)

            for column in inspector.get_columns(table):
                if column["name"] == column_name:
                    return column["type"].length

            return -1

        # Alter columns.
        session.execute("ALTER TABLE job ALTER COLUMN hostname TYPE VARCHAR(1);")
        session.execute("ALTER TABLE task_instance ALTER COLUMN hostname TYPE VARCHAR(1);")
        session.commit()
        assert _length_of_column("job", "hostname") == 1
        assert _length_of_column("task_instance", "hostname") == 1

        _apply_composer_migrations()

        assert _length_of_column("job", "hostname") == 100
        assert _length_of_column("task_instance", "hostname") == 100

    def test_apply_composer_migrations_twice_does_not_fail(self):
        _apply_composer_migrations()
        _apply_composer_migrations()
