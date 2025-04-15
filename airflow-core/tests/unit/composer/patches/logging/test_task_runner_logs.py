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

from unittest import mock

from structlog._frames import _format_exception
from structlog.processors import ExceptionRenderer

from airflow.composer.patches.logging.task_runner_logs import patch_task_runner_log_processors


class TestTaskRunnerLogs:
    @mock.patch("airflow.composer.patches.logging.task_runner_logs.get_config", autospec=True)
    def test_patch_task_runner_log_processors(self, get_config_mock):
        exception_renderer = ExceptionRenderer()
        exception_renderer.format_exception = mock.Mock()
        processors = ["test-processor-1", exception_renderer, "test-processor-2"]
        get_config_mock.return_value = {"processors": processors}

        patch_task_runner_log_processors()

        assert len(processors) == 3
        assert processors[0] == "test-processor-1"
        assert isinstance(processors[1], ExceptionRenderer)
        assert processors[1].format_exception is _format_exception
        assert processors[2] == "test-processor-2"

    @mock.patch("airflow.composer.patches.logging.task_runner_logs.get_config", autospec=True)
    def test_patch_task_runner_log_processors_empty(self, get_config_mock):
        processors = []
        get_config_mock.return_value = {"processors": processors}

        patch_task_runner_log_processors()

        assert processors == []
