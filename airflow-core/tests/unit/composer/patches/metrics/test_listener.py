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
from datetime import datetime
from unittest import mock

from airflow.composer.patches.metrics.listener import (
    on_dag_run_failed,
    on_dag_run_success,
)
from airflow.models import DagRun, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.utils.session import provide_session


@provide_session
def _create_dummy_task_instance(dag_id, task_id, duration, session):
    run_id = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
    dr = DagRun(
        dag_id=dag_id,
        run_id=run_id,
        run_type="manual",
    )
    ti = TaskInstance(
        task=BaseOperator(task_id=task_id),
        run_id=run_id,
    )
    ti.dag_id = dag_id
    ti.duration = duration

    session.add(dr)
    session.add(ti)
    session.commit()

    return ti


class TestListener:
    @mock.patch("airflow.composer.patches.metrics.listener.Stats.incr", autospec=True)
    @mock.patch("airflow.composer.patches.metrics.listener.Stats.gauge", autospec=True)
    def test_on_dag_run_success(self, gauge_mock, incr_mock):
        on_dag_run_success(
            dag_run=mock.Mock(
                dag_id="test-dag",
                state="success",
                start_date=datetime(2010, 1, 1),
                end_date=datetime(2010, 1, 2),
            ),
            msg="message",
        )

        incr_mock.assert_called_once_with("workflow.count.test-dag@-@success", 1)
        gauge_mock.assert_called_once_with("workflow.duration.test-dag@-@success", 86400)

    @mock.patch("airflow.composer.patches.metrics.listener.Stats.incr", autospec=True)
    @mock.patch("airflow.composer.patches.metrics.listener.Stats.gauge", autospec=True)
    def test_on_dag_run_failed(self, gauge_mock, incr_mock):
        on_dag_run_failed(
            dag_run=mock.Mock(
                dag_id="test-dag",
                state="failed",
                start_date=datetime(2010, 1, 1),
                end_date=datetime(2010, 1, 2),
            ),
            msg="message",
        )

        incr_mock.assert_called_once_with("workflow.count.test-dag@-@failed", 1)
        gauge_mock.assert_called_once_with("workflow.duration.test-dag@-@failed", 86400)

    @mock.patch("airflow.composer.patches.metrics.listener.Stats.incr", autospec=True)
    @mock.patch("airflow.composer.patches.metrics.listener.Stats.gauge", autospec=True)
    def test_on_dag_run_failed_no_end_date(self, gauge_mock, incr_mock):
        on_dag_run_failed(
            dag_run=mock.Mock(
                dag_id="test-dag",
                state="failed",
                start_date=datetime(2010, 1, 1),
                end_date=None,
            ),
            msg="message",
        )

        incr_mock.assert_called_once_with("workflow.count.test-dag@-@failed", 1)
        gauge_mock.assert_not_called()
