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

AIRFLOW_SDK_EXECUTION_TIME_TASK_RUNNER_PATCH_PATH = (
    "airflow.composer.patches.metrics.monkey_patching.airflow_sdk_execution_time_task_runner"
)


class TestAirflowSdkExecutionTimeTaskRunner:
    @mock.patch(
        f"{AIRFLOW_SDK_EXECUTION_TIME_TASK_RUNNER_PATCH_PATH}._composer_task_runner_run",
    )
    @mock.patch(
        f"{AIRFLOW_SDK_EXECUTION_TIME_TASK_RUNNER_PATCH_PATH}._composer_task_runner_handle_current_task_failed",
    )
    def test_patch(self, composer_task_runner_handle_current_task_failed_mock, composer_task_runner_run_mock):
        composer_task_runner_handle_current_task_failed_mock.assert_not_called()
        composer_task_runner_run_mock.assert_not_called()

        patch()

        composer_task_runner_handle_current_task_failed_mock.assert_called_once()
        composer_task_runner_run_mock.assert_called_once()

    @mock.patch(
        "airflow.sdk.execution_time.task_runner.run",
        return_value=("mocked-state", "mocked-msg", "mocked-error"),
    )
    @mock.patch(
        f"{AIRFLOW_SDK_EXECUTION_TIME_TASK_RUNNER_PATCH_PATH}.emit_metrics_on_task_instance_finished",
        autospec=True,
    )
    def test_emit_metrics_on_task_instance_finished(self, emit_metrics_on_task_instance_finished_mock, _):
        patch()
        run_id = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        ti = TaskInstance(
            task=BaseOperator(task_id="test-task-id"),
            dag_version_id="test-version",
            run_id=run_id,
        )

        res = task_runner.run(ti)

        assert res == ("mocked-state", "mocked-msg", "mocked-error")
        emit_metrics_on_task_instance_finished_mock.assert_called_once_with(ti, "mocked-state", "mocked-msg")

    @mock.patch(
        "airflow.sdk.execution_time.task_runner._handle_current_task_failed",
        return_value=("mocked-task-state", "mocked-ti-state"),
    )
    @mock.patch(
        f"{AIRFLOW_SDK_EXECUTION_TIME_TASK_RUNNER_PATCH_PATH}.emit_metrics_on_task_failed",
        autospec=True,
    )
    def test_emit_metrics_on_task_failed(self, emit_metrics_on_task_failed_mock, _):
        patch()
        run_id = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        ti = TaskInstance(
            task=BaseOperator(task_id="test-task-id"),
            dag_version_id="test-version",
            run_id=run_id,
        )

        res = task_runner._handle_current_task_failed(ti)

        assert res == ("mocked-task-state", "mocked-ti-state")
        emit_metrics_on_task_failed_mock.assert_called_once_with(ti)
