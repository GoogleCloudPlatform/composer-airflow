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

from airflow.models.taskinstance import TaskInstance


def patch():
    if not hasattr(TaskInstance.hostname.type, "_old_length"):
        TaskInstance.hostname.type._old_length = TaskInstance.hostname.type.length

    TaskInstance.hostname.type.length = 100
    TaskInstance.__table_args__ = TaskInstance.__table_args__ + (
        Index(
            "ti_worker_healthcheck",
            TaskInstance.end_date,
            TaskInstance.hostname,
            TaskInstance.state,
            unique=False,
        ),
    )
