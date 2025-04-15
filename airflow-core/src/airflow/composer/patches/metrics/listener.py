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

import functools
from typing import TYPE_CHECKING

from airflow.listeners import hookimpl
from airflow.metrics.validators import ALLOWED_CHARACTERS, stat_name_default_handler
from airflow.stats import Stats

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun


# We use `@` in names of Composer metrics.
stat_name_handler = functools.partial(
    stat_name_default_handler, allowed_chars=set(ALLOWED_CHARACTERS).union({"@"})
)


@hookimpl
def on_dag_run_success(dag_run: DagRun, msg: str):
    _emit_metrics_on_dag_run_finished(dag_run=dag_run)


@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str):
    _emit_metrics_on_dag_run_finished(dag_run=dag_run)


def _emit_metrics_on_dag_run_finished(dag_run: DagRun):
    Stats.incr(f"workflow.count.{dag_run.dag_id}@-@{dag_run.state}", 1)
    if dag_run.start_date and dag_run.end_date:
        Stats.gauge(
            f"workflow.duration.{dag_run.dag_id}@-@{dag_run.state}",
            (dag_run.end_date - dag_run.start_date).total_seconds(),
        )
