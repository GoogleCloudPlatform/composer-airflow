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

from urllib.parse import urljoin

import requests

from integration.composer.utils import API_SERVER_URL


class TestAirflowLocalSettings:
    def test_airflow_local_settings(self):
        """
        This test verifies that Composer airflow_local_settings.py is used by Airflow in integration tests.

        We check that Composer airflow_local_settings.py is used by verifying that Composer plugins are
        registered.
        """
        plugins_url = urljoin(API_SERVER_URL, "/api/v2/plugins")

        response = requests.get(plugins_url)

        assert response.status_code == 200
        plugin_names = [plugin["name"] for plugin in response.json()["plugins"]]
        assert "ComposerMetricsPlugin" in plugin_names
        assert "ComposerMenuPlugin" in plugin_names
