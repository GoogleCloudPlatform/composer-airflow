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

import structlog

from airflow.dag_processing.manager import DagFileProcessorManager


def patch():
    DagFileProcessorManager._get_logger_for_dag_file = _composer_get_logger_for_dag_file(
        DagFileProcessorManager._get_logger_for_dag_file
    )


def _composer_get_logger_for_dag_file(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        res = f(*args, **kwargs)

        # Route DAG files processing logs to /dev/null to avoid storing them on disk and exceeding DAG
        # processor storage.
        res[0]._logger = structlog.BytesLogger(open("/dev/null", "ab"))

        return res

    return wrapper
