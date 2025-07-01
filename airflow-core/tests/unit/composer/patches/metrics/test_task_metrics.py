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

from airflow.composer.patches.metrics.task_metrics import emit_metrics_on_task_instance_finished
from airflow.models import TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.sdk.api.datamodels._generated import (
    IntermediateTIState,
    TerminalTIState,
)
from airflow.sdk.execution_time.comms import (
    DeferTask,
    SucceedTask,
    TaskState,
)


def _create_dummy_task_instance(dag_id, task_id, start_date):
    ti = TaskInstance(
        run_id="".join(random.choice(string.ascii_uppercase) for _ in range(6)),
        task=BaseOperator(task_id=task_id),
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
                TerminalTIState.SUCCESS,
                SucceedTask(end_date=END_DATE, task_outlets=[], outlet_events=[]),
                "success",
            ),
            (TerminalTIState.FAILED, TaskState(state=TerminalTIState.FAILED, end_date=END_DATE), "failed"),
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
        state = TerminalTIState.FAILED
        msg = TaskState(state=TerminalTIState.FAILED, end_date=None)

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
        state = IntermediateTIState.DEFERRED
        msg = DeferTask(
            classpath="Triggerer",
            next_method="next_method",
        )

        emit_metrics_on_task_instance_finished(ti, state, msg)

        incr_mock.assert_not_called()
        gauge_mock.assert_not_called()
