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

from sqlalchemy import Index

from airflow.composer.patches.database_migrations.monkey_patching.airflow_models_taskinstance import patch
from airflow.models.taskinstance import TaskInstance


class TestAirflowJobsJob:
    @classmethod
    def setup_class(cls):
        patch()

    def test_patch(self):
        assert TaskInstance.hostname.type.length == 100

        # Test community index(es) preserved.
        index_names = [
            table_arg.name for table_arg in TaskInstance.__table_args__ if isinstance(table_arg, Index)
        ]
        assert "ti_dag_state" in index_names

        ti_worker_healthcheck_index = None
        for table_arg in TaskInstance.__table_args__:
            if isinstance(table_arg, Index) and table_arg.name == "ti_worker_healthcheck":
                ti_worker_healthcheck_index = table_arg
                break
        assert len(ti_worker_healthcheck_index.expressions) == 3
        assert ti_worker_healthcheck_index.expressions[0].name == "end_date"
        assert ti_worker_healthcheck_index.expressions[1].name == "hostname"
        assert ti_worker_healthcheck_index.expressions[2].name == "state"
        assert ti_worker_healthcheck_index.unique is False

    def test_taskinstance_hostname_old_length(self):
        """Test to assure that TaskInstance.hostname column length has expected value in community code.

        Once this test fails, we should revisit our Composer patch for this field.
        """
        assert TaskInstance.hostname.type._old_length == 1000
