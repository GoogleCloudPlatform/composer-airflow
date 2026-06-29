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

from airflow.composer.patches.metrics import listener
from airflow.composer.patches.metrics.plugin import (
    ComposerMetricsPlugin,
    ComposerMetricsPluginSource,
    register_composer_metrics_plugin,
)


class TestPlugin:
    @mock.patch("airflow.composer.patches.metrics.plugin._get_plugins", autospec=True)
    def test_register_composer_metrics_plugin(self, get_plugins_mock):
        plugins_list = []
        get_plugins_mock.return_value = (plugins_list, {})

        register_composer_metrics_plugin()

        assert len(plugins_list) == 1
        actual_plugin = plugins_list[0]
        assert isinstance(actual_plugin, ComposerMetricsPlugin)
        assert actual_plugin.listeners == [listener]
        assert isinstance(actual_plugin.source, ComposerMetricsPluginSource)

    def test_composer_metrics_plugin_source(self):
        source = ComposerMetricsPluginSource()

        assert str(source) == "airflow.composer.patches.metrics.plugin"
        assert source.__html__() == "airflow.composer.patches.metrics.plugin"
