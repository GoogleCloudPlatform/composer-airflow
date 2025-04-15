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

from airflow.composer.patches.logging.supervisor_logs import (
    patch_supervisor_log_processors,
    patch_supervisor_stdlib_logging_configuration,
)
from airflow.composer.patches.logging.task_runner_logs import patch_task_runner_log_processors
from airflow.sdk import log


def patch():
    log.configure_logging = _composer_log_configure_logging(log.configure_logging)


def _composer_log_configure_logging(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        res = f(*args, **kwargs)

        # Note, that configure_logging method is called for both supervisor and task runner processes. For
        # task runner process it is called with sending_to_supervisor=True.
        if not kwargs.get("sending_to_supervisor"):
            patch_supervisor_log_processors()
            patch_supervisor_stdlib_logging_configuration()
        else:
            patch_task_runner_log_processors()

        return res

    return wrapper
