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

from typing import TYPE_CHECKING

from airflow.sdk.api.datamodels._generated import (
    IntermediateTIState,
    TerminalTIState,
)
from airflow.stats import Stats

if TYPE_CHECKING:
    from airflow.sdk.execution_time.comms import ToSupervisor
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance


def emit_metrics_on_task_instance_finished(
    task_instance: RuntimeTaskInstance, state: IntermediateTIState | TerminalTIState, msg: ToSupervisor
):
    """Emit metrics when task instance execution is finished."""
    if state not in TerminalTIState:
        return

    Stats.incr(
        (
            f"task.count.{task_instance.dag_id}@-@{task_instance.task_id}@-@{task_instance.task.task_type}@-@"
            f"{state.value}@-@{task_instance.task.queue}"
        ),
        1,
    )

    if msg.end_date:
        duration = msg.end_date - task_instance.start_date
        Stats.gauge(
            (
                f"task.duration.{task_instance.dag_id}@-@{task_instance.task_id}@-@"
                f"{task_instance.task.task_type}@-@{state.value}"
            ),
            duration.total_seconds(),
        )
