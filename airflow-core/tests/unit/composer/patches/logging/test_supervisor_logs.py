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

import logging
from unittest import mock

from structlog import BytesLogger
from structlog.processors import CallsiteParameter, CallsiteParameterAdder

from airflow.composer.patches.logging.supervisor_logs import (
    get_task_logs_contextvars,
    patch_supervisor_log_processors,
    patch_supervisor_stdlib_logging_configuration,
    supervisor_log_processor,
)


class TestSupervisorLogs:
    def test_get_task_logs_contextvars(self):
        actual = get_task_logs_contextvars(
            mock.Mock(dag_id="dag-id", task_id="task-id", run_id="run-id", map_index=-1, try_number=1)
        )

        assert actual == {
            "composer_ti_info": {
                "workflow": "dag-id",
                "task-id": "task-id",
                "run-id": "run-id",
                "map-index": "-1",
                "try-number": "1",
            }
        }

    @mock.patch("airflow.composer.patches.logging.supervisor_logs.get_config", autospec=True)
    def test_patch_supervisor_log_processors(self, get_config_mock):
        processors = ["test-processor"]
        get_config_mock.return_value = {"processors": processors}

        patch_supervisor_log_processors()

        assert len(processors) == 2
        assert isinstance(processors[0], CallsiteParameterAdder)
        assert [ah[0] for ah in processors[0]._active_handlers] == [
            CallsiteParameter.FILENAME,
            CallsiteParameter.FUNC_NAME,
            CallsiteParameter.LINENO,
        ]
        assert processors[1] is supervisor_log_processor

    def test_supervisor_log_processor_composer_ti_info(self):
        actual = supervisor_log_processor(
            "logger",
            "method-name",
            {
                "event": "message",
                "timestamp": "2023-01-03 22:34:56,123",
                "filename": "module.py",
                "lineno": 123,
                "level": "info",
                "func_name": "execute_task",
                "composer_ti_info": {
                    "workflow": "test-dag",
                    "task-id": "test-task",
                },
            },
        )

        assert actual == (
            "[2023-01-03 22:34:56,123] {module.py:123} INFO - message"
            '@-@{"function": "execute_task", "workflow": "test-dag", "task-id": "test-task"}'
        )

    def test_supervisor_log_processor_composer_extra_info(self):
        actual = supervisor_log_processor(
            "logger",
            "method-name",
            {
                "event": "message",
                "timestamp": "2023-01-03 22:34:56,123",
                "filename": "module.py",
                "lineno": 123,
                "level": "info",
                "func_name": "execute_task",
                "composer_ti_info": {
                    "workflow": "test-dag",
                },
                "composer_extra_info": {
                    "extra-field": "extra-value",
                },
            },
        )

        assert actual == (
            "[2023-01-03 22:34:56,123] {module.py:123} INFO - message"
            '@-@{"function": "execute_task", "workflow": "test-dag", "extra-field": "extra-value"}'
        )

    def test_supervisor_log_processor_line_length(self):
        actual = supervisor_log_processor(
            "logger",
            "method-name",
            {
                "event": "A" * 4100,
                "timestamp": "2023-01-03 22:34:56,123",
                "filename": "module.py",
                "lineno": 123,
                "level": "info",
                "func_name": "execute_task",
            },
        )

        assert actual == (
            f"[2023-01-03 22:34:56,123] {{module.py:123}} INFO - {'A' * 4096}"
            '@-@{"function": "execute_task"}\n'
            f"[2023-01-03 22:34:56,123] {{module.py:123}} INFO - {'A' * 4}"
            '@-@{"function": "execute_task"}'
        )

    def test_supervisor_log_processor_escaped_lines(self):
        actual = supervisor_log_processor(
            "logger",
            "method-name",
            {
                "event": "\\ aaa \n bbb \r",
                "timestamp": "2023-01-03 22:34:56,123",
                "filename": "module.py",
                "lineno": 123,
                "level": "info",
                "func_name": "execute_task",
            },
        )

        assert actual == (
            "[2023-01-03 22:34:56,123] {module.py:123} INFO - \\\\ aaa \\n bbb \\r"
            '@-@{"function": "execute_task"}'
        )

    def test_supervisor_log_processor_empty_event(self):
        actual = supervisor_log_processor(
            "logger",
            "method-name",
            {
                "event": "",
                "timestamp": "2023-01-03 22:34:56,123",
                "filename": "module.py",
                "lineno": 123,
                "level": "info",
                "func_name": "execute_task",
            },
        )

        assert actual == ('[2023-01-03 22:34:56,123] {module.py:123} INFO - @-@{"function": "execute_task"}')

    def test_supervisor_log_processor_error_detail(self):
        actual = supervisor_log_processor(
            "logger",
            "method-name",
            {
                "event": "Task failed with exception",
                "timestamp": "2023-01-03 22:34:56,123",
                "filename": "module.py",
                "lineno": 123,
                "level": "info",
                "func_name": "execute_task",
                "error_detail": "Traceback (most recent call last): ValueError: aaa",
            },
        )

        assert actual == (
            "[2023-01-03 22:34:56,123] {module.py:123} INFO - Task failed with exception\\nTraceback (most recent call last): ValueError: aaa"
            '@-@{"function": "execute_task"}'
        )

    def test_supervisor_log_processor_bytes_logger(self):
        actual = supervisor_log_processor(
            BytesLogger(),
            "method-name",
            {
                "event": "Text",
                "timestamp": "2023-01-03 22:34:56,123",
                "filename": "module.py",
                "lineno": 123,
                "level": "info",
                "func_name": "execute_task",
            },
        )

        assert actual == (
            b'[2023-01-03 22:34:56,123] {module.py:123} INFO - Text@-@{"function": "execute_task"}'
        )

    @mock.patch("logging.root", autospec=True)
    def test_patch_supervisor_stdlib_logging_configuration(self, root_mock):
        set_formatter_mock = mock.Mock()
        root_mock.handlers = [mock.Mock(setFormatter=set_formatter_mock)]

        patch_supervisor_stdlib_logging_configuration()

        assert len(set_formatter_mock.call_args_list) == 1
        assert isinstance(set_formatter_mock.call_args_list[0][0][0], logging.Formatter)
        assert (
            set_formatter_mock.call_args_list[0][0][0]._fmt
            == "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
        )
