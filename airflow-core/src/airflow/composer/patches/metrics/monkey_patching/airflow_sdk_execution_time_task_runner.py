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

from airflow.composer.patches.metrics.task_metrics import emit_metrics_on_task_instance_finished
from airflow.sdk.execution_time import task_runner


def patch():
    task_runner.run = _composer_task_runner_run(task_runner.run)


def _composer_task_runner_run(f):
    @functools.wraps(f)
    def wrapper(ti, *args, **kwargs):
        state, msg, error = f(ti, *args, **kwargs)

        emit_metrics_on_task_instance_finished(ti, state, msg)

        return state, msg, error

    return wrapper
