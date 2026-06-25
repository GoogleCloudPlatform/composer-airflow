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

from airflow.composer.patches.metrics import listener
from airflow.plugins_manager import AirflowPlugin, AirflowPluginSource, register_plugin


def register_composer_metrics_plugin():
    plugin_instance = ComposerMetricsPlugin()
    plugin_instance.source = ComposerMetricsPluginSource()
    register_plugin(plugin_instance)


class ComposerMetricsPluginSource(AirflowPluginSource):
    """Class to define ComposerMetricsPlugin source metadata."""

    def __str__(self):
        return "airflow.composer.patches.metrics.plugin"

    def __html__(self):
        return "airflow.composer.patches.metrics.plugin"


class ComposerMetricsPlugin(AirflowPlugin):
    """Airflow plugin for emitting Composer metrics."""

    name = "ComposerMetricsPlugin"
    listeners = [listener]
