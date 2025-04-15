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

from airflow.composer.patches.logging.monkey_patching.airflow_sdk_log import patch
from airflow.sdk import log


class TestAirflowSdkLog:
    @mock.patch("airflow.sdk.log.configure_logging", return_value=123)
    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_sdk_log.patch_supervisor_log_processors",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_sdk_log.patch_supervisor_stdlib_logging_configuration",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_sdk_log.patch_task_runner_log_processors",
        autospec=True,
    )
    def test_patch_no_sending_to_supervisor_kwarg(
        self,
        patch_task_runner_log_processors_mock,
        patch_supervisor_stdlib_logging_configuration_mock,
        patch_supervisor_log_processors_mock,
        configure_logging_mock,
    ):
        patch()

        res = log.configure_logging()

        assert res == 123
        patch_supervisor_log_processors_mock.assert_called_once_with()
        patch_supervisor_stdlib_logging_configuration_mock.assert_called_once_with()
        patch_task_runner_log_processors_mock.assert_not_called()

    @mock.patch("airflow.sdk.log.configure_logging", return_value=123)
    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_sdk_log.patch_supervisor_log_processors",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_sdk_log.patch_supervisor_stdlib_logging_configuration",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_sdk_log.patch_task_runner_log_processors",
        autospec=True,
    )
    def test_patch_sending_to_supervisor_false(
        self,
        patch_task_runner_log_processors_mock,
        patch_supervisor_stdlib_logging_configuration_mock,
        patch_supervisor_log_processors_mock,
        configure_logging_mock,
    ):
        patch()

        res = log.configure_logging(sending_to_supervisor=False)

        assert res == 123
        patch_supervisor_log_processors_mock.assert_called_once_with()
        patch_supervisor_stdlib_logging_configuration_mock.assert_called_once_with()
        patch_task_runner_log_processors_mock.assert_not_called()

    @mock.patch("airflow.sdk.log.configure_logging", return_value=123)
    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_sdk_log.patch_supervisor_log_processors",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_sdk_log.patch_supervisor_stdlib_logging_configuration",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_sdk_log.patch_task_runner_log_processors",
        autospec=True,
    )
    def test_patch_sending_to_supervisor_true(
        self,
        patch_task_runner_log_processors_mock,
        patch_supervisor_stdlib_logging_configuration_mock,
        patch_supervisor_log_processors_mock,
        configure_logging_mock,
    ):
        patch()

        res = log.configure_logging(sending_to_supervisor=True)

        assert res == 123
        patch_supervisor_log_processors_mock.assert_not_called()
        patch_supervisor_stdlib_logging_configuration_mock.assert_not_called()
        patch_task_runner_log_processors_mock.assert_called_once_with()
