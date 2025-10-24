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
from datetime import datetime, timezone
from unittest import mock

import pytest

from airflow.composer.patches.metrics.task_metrics import (
    emit_metrics_on_task_failed,
    emit_metrics_on_task_instance_finished,
)
from airflow.models import TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.api.datamodels._generated import (
    TaskInstanceState,
)
from airflow.sdk.execution_time.comms import (
    DeferTask,
    SucceedTask,
    TaskState,
)


def _create_dummy_task_instance(dag_id, task_id, start_date, task=None):
    if task is None:
        task = BaseOperator(task_id=task_id)
    ti = TaskInstance(
        task=task,
        dag_version_id="test-version",
        run_id="".join(random.choice(string.ascii_uppercase) for _ in range(6)),
    )
    ti.dag_id = dag_id
    ti.start_date = start_date
    return ti


START_DATE = datetime(2025, 7, 10, 15, 10, 35, tzinfo=timezone.utc)
END_DATE = datetime(2025, 7, 10, 15, 12, 40, tzinfo=timezone.utc)


class TestTaskMetrics:
    @mock.patch("airflow.composer.patches.metrics.listener.Stats.incr", autospec=True)
    @mock.patch("airflow.composer.patches.metrics.listener.Stats.gauge", autospec=True)
    @pytest.mark.parametrize(
        "state, msg, status",
        [
            (
                TaskInstanceState.SUCCESS,
                SucceedTask(end_date=END_DATE, task_outlets=[], outlet_events=[]),
                "success",
            ),
            (
                TaskInstanceState.FAILED,
                TaskState(state=TaskInstanceState.FAILED, end_date=END_DATE),
                "failed",
            ),
        ],
    )
    def test_emit_metrics_on_task_instance_finished(self, gauge_mock, incr_mock, state, msg, status):
        ti = _create_dummy_task_instance("test-dag", "test-task", START_DATE)

        emit_metrics_on_task_instance_finished(ti, state, msg)

        incr_mock.assert_called_once_with(
            f"task.count.test-dag@-@test-task@-@BaseOperator@-@{status}@-@default",
            1,
        )
        gauge_mock.assert_called_once_with(
            f"task.duration.test-dag@-@test-task@-@BaseOperator@-@{status}",
            125,
        )

    @mock.patch("airflow.composer.patches.metrics.listener.Stats.incr", autospec=True)
    @mock.patch("airflow.composer.patches.metrics.listener.Stats.gauge", autospec=True)
    def test_emit_metrics_on_task_instance_finished_no_end_date(self, gauge_mock, incr_mock):
        ti = _create_dummy_task_instance("test-dag", "test-task", START_DATE)
        state = TaskInstanceState.FAILED
        msg = TaskState(state=TaskInstanceState.FAILED, end_date=None)

        emit_metrics_on_task_instance_finished(ti, state, msg)

        incr_mock.assert_called_once_with(
            "task.count.test-dag@-@test-task@-@BaseOperator@-@failed@-@default",
            1,
        )
        gauge_mock.assert_not_called()

    @mock.patch("airflow.composer.patches.metrics.listener.Stats.incr", autospec=True)
    @mock.patch("airflow.composer.patches.metrics.listener.Stats.gauge", autospec=True)
    def test_emit_metrics_on_task_instance_finished_itermediate_ti_state(self, gauge_mock, incr_mock):
        ti = _create_dummy_task_instance("test-dag", "test-task", START_DATE)
        state = TaskInstanceState.DEFERRED
        msg = DeferTask(
            classpath="Triggerer",
            next_method="next_method",
        )

        emit_metrics_on_task_instance_finished(ti, state, msg)

        incr_mock.assert_not_called()
        gauge_mock.assert_not_called()

    @mock.patch("airflow.composer.patches.metrics.listener.Stats.incr", autospec=True)
    @pytest.mark.parametrize(
        "dag_id, task_id, task, expected_task_class_name",
        [
            ("test-dag-id", "test-task-id", BaseOperator(task_id="test-task-id"), "BaseOperator"),
            (
                "test-dag-id-2",
                "test-task-id-2",
                PythonOperator(task_id="test-task-id-2", python_callable=lambda: 0),
                "PythonOperator",
            ),
            (
                "test-dag-id-3",
                "test-task-id-3",
                BashOperator(task_id="test-task-id-3", bash_command="echo 0"),
                "BashOperator",
            ),
        ],
    )
    def test_emit_metrics_on_task_instance_failed(
        self, incr_mock, dag_id, task_id, task, expected_task_class_name
    ):
        ti = _create_dummy_task_instance(dag_id, task_id, START_DATE, task)

        emit_metrics_on_task_failed(ti)

        expected_stats_tags = {"dag_id": dag_id, "task_id": task_id}
        incr_mock.assert_any_call(f"operator_failures_{expected_task_class_name}", tags=expected_stats_tags)
        incr_mock.assert_any_call("ti_failures", tags=expected_stats_tags)
        assert incr_mock.call_count == 2
