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

from airflow.composer.patches.database_retention.tables import (
    _compile_table_data,
    get_table_primary_key,
    tables_to_trim,
)
from airflow.jobs.job import Job
from airflow.models import TaskInstance


class TestTables:
    def test_tables_to_trim_order(self):
        tables = tables_to_trim()

        assert [t["airflow_db_model"].__tablename__ for t in tables] == [
            "job",
            "log",
            "import_error",
            "xcom",
            "rendered_task_instance_fields",
            "task_instance",
            "dag_run",
        ]

    def test_get_table_primary_key_id(self):
        assert get_table_primary_key(_compile_table_data(Job, Job.latest_heartbeat)) == [Job.id]

    def test_get_table_primary_key_custom(self):
        assert get_table_primary_key(
            _compile_table_data(
                TaskInstance,
                TaskInstance.logical_date,
                {
                    "primary_key": [
                        TaskInstance.dag_id,
                        TaskInstance.task_id,
                        TaskInstance.run_id,
                        TaskInstance.map_index,
                    ],
                },
            )
        ) == [
            TaskInstance.dag_id,
            TaskInstance.task_id,
            TaskInstance.run_id,
            TaskInstance.map_index,
        ]
