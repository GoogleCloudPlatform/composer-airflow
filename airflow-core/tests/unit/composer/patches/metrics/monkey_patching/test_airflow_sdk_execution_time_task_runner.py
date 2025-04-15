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

import random
import string
from unittest import mock

from airflow.composer.patches.metrics.monkey_patching.airflow_sdk_execution_time_task_runner import patch
from airflow.models import TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.sdk.execution_time import task_runner


class TestAirflowPluginsManager:
    @mock.patch(
        "airflow.sdk.execution_time.task_runner.run",
        return_value=("mocked-state", "mocked-msg", "mocked-error"),
    )
    @mock.patch(
        "airflow.composer.patches.metrics.monkey_patching.airflow_sdk_execution_time_task_runner.emit_metrics_on_task_instance_finished",
        autospec=True,
    )
    def test_patch(self, emit_metrics_on_task_instance_finished_mock, ensure_plugins_loaded_mock):
        patch()
        run_id = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        ti = TaskInstance(
            task=BaseOperator(task_id="test-task-id"),
            run_id=run_id,
        )

        res = task_runner.run(ti)

        assert res == ("mocked-state", "mocked-msg", "mocked-error")
        emit_metrics_on_task_instance_finished_mock.assert_called_once_with(ti, "mocked-state", "mocked-msg")
