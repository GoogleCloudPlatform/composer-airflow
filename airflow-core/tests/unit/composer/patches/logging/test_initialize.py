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

from airflow.composer.patches.logging.initialize import initialize


class TestInitialize:
    @mock.patch(
        "airflow.composer.patches.logging.triggerer_logs.log_slow_callbacks",
        autospec=True,
    )
    def test_initialize(self, log_slow_callbacks_mock):
        initialize()

        log_slow_callbacks_mock.assert_called_once_with()
