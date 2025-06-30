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


class TestAirflowLocalSettings:
    @mock.patch("airflow.composer.patches.core.initialize.initialize", autospec=True)
    def test_initialize_called_on_import(self, initialize_mock):
        initialize_mock.assert_not_called()

        from airflow.composer.patches.core import airflow_local_settings  # noqa: F401

        initialize_mock.assert_called_once()

    def test_pod_mutation_hook_defined(self):
        from airflow.composer.patches.core import airflow_local_settings

        airflow_local_settings.pod_mutation_hook(mock.Mock())
