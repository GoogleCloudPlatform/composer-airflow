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

from airflow import plugins_manager
from airflow.composer.patches.webserver.monkey_patching.airflow_plugins_manager import patch


class TestAirflowPluginsManager:
    @mock.patch("airflow.plugins_manager._get_plugins", return_value=(["mocked"], {}))
    @mock.patch(
        "airflow.composer.patches.webserver.monkey_patching.airflow_plugins_manager.get_composer_menu_plugin",
        autospec=True,
    )
    def test_patch(self, get_composer_menu_plugin_mock, _get_plugins_mock):
        mock_plugin = mock.MagicMock()
        get_composer_menu_plugin_mock.return_value = mock_plugin

        patch()

        res = plugins_manager._get_plugins()

        assert res[0] == ["mocked", mock_plugin]
        assert res[1] == {}
