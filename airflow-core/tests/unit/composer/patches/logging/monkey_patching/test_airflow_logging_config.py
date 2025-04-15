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

from airflow import logging_config
from airflow.composer.patches.logging.monkey_patching.airflow_logging_config import patch


class TestAirflowLoggingConfig:
    @mock.patch("airflow.logging_config.configure_logging", return_value="mocked")
    @mock.patch(
        "airflow.composer.patches.logging.monkey_patching.airflow_logging_config.filter_warnings",
        autospec=True,
    )
    def test_patch(self, filter_warnings_mock, configure_logging_mock):
        patch()

        res = logging_config.configure_logging()

        assert res == "mocked"
        filter_warnings_mock.assert_called_once_with()
