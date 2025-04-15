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

from airflow.composer.patches.logging.triggerer_logs import log_slow_callbacks


class TestTriggererLogs:
    @mock.patch("sys.argv", ["airflow", "triggerer"])
    @mock.patch("aiodebug.log_slow_callbacks.enable", autospec=True)
    def test_log_slow_callbacks_triggerer_running(self, enable_mock):
        log_slow_callbacks()

        enable_mock.assert_called_once_with(0.5)

    @mock.patch("sys.argv", ["airflow", "scheduler"])
    @mock.patch("aiodebug.log_slow_callbacks.enable", autospec=True)
    def test_log_slow_callbacks_scheduler_running(self, enable_mock):
        log_slow_callbacks()

        enable_mock.assert_not_called()
