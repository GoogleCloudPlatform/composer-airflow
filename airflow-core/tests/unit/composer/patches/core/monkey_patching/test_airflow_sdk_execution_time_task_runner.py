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

import pytest

from airflow.composer.patches.core.monkey_patching.airflow_sdk_execution_time_task_runner import patch
from airflow.sdk.execution_time import task_runner

from tests_common.test_utils.config import conf_vars


class TestAirflowSdkExecutionTimeTaskRunner:
    @mock.patch("airflow.sdk.execution_time.task_runner.parse", autospec=True)
    def test_task_runner_parse_returns_result_from_original_method(self, parse_mock):
        patch()
        parse_mock.return_value = "runtime_task_instance"

        actual_result = task_runner.parse("what_arg", "log_arg")

        parse_mock.assert_called_once_with("what_arg", "log_arg")
        assert actual_result == "runtime_task_instance"

    @mock.patch("airflow.sdk.execution_time.task_runner.parse", autospec=True)
    @mock.patch("time.sleep", autospec=True)
    @conf_vars({("core", "wait_dag_not_found_timeout"): "10"})
    def test_task_runner_parse_parsing_error_once(self, sleep_mock, parse_mock):
        patch()

        def parse_side_effect(what, log):
            parse_side_effect.call_counter += 1
            if parse_side_effect.call_counter == 1:
                raise SystemExit("Parsing error")
            return "second_time_parsed_ok"

        parse_side_effect.call_counter = 0
        parse_mock.side_effect = parse_side_effect

        actual_result = task_runner.parse("what_arg", "log_arg")

        assert parse_mock.call_args_list == [
            mock.call("what_arg", "log_arg"),
            mock.call("what_arg", "log_arg"),
        ]
        sleep_mock.assert_called_once_with(5)
        assert actual_result == "second_time_parsed_ok"

    @mock.patch("airflow.sdk.execution_time.task_runner.parse", autospec=True)
    @mock.patch("time.sleep", autospec=True)
    @conf_vars(
        {("core", "wait_dag_not_found_timeout"): "1"}
    )  # "parse" method will raise SystemExit after 1 second timeout.
    def test_task_runner_parse_parsing_error_constantly(self, sleep_mock, parse_mock):
        patch()
        parse_mock.side_effect = SystemExit("Parsing error constantly")

        with pytest.raises(SystemExit) as exc:
            task_runner.parse("what_arg", "log_arg")

        parse_mock.assert_called_with("what_arg", "log_arg")
        sleep_mock.assert_called_with(5)
        assert exc.value.code == 1
