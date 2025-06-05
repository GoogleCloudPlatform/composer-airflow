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

import os
from importlib import reload
from unittest import mock

import pytest

from airflow.composer.patches.webserver import composer_menu_plugin
from airflow.composer.patches.webserver.composer_menu_plugin import register_composer_menu_plugin


class TestComposerMenuPlugin:
    @classmethod
    def setup_class(cls):
        with mock.patch.dict(
            os.environ,
            {
                "GCP_PROJECT": "test-project",
                "COMPOSER_LOCATION": "test-location",
                "COMPOSER_ENVIRONMENT": "test-env",
                "GCS_BUCKET": "test-location-test-env-bucket",
            },
        ):
            reload(composer_menu_plugin)

    @pytest.mark.parametrize(
        "expected_label, expected_href",
        [
            (
                "DAGs in Cloud Console",
                (
                    "https://console.cloud.google.com/composer/environments/detail/test-location/test-env"
                    "/dags?project=test-project"
                ),
            ),
            (
                "DAGs in Cloud Storage",
                "https://console.cloud.google.com/storage/browser/test-location-test-env-bucket/dags",
            ),
            (
                "Environment Monitoring",
                (
                    "https://console.cloud.google.com/composer/environments/detail/test-location/test-env"
                    "/monitoring?project=test-project"
                ),
            ),
            (
                "Environment Logs",
                (
                    "https://console.cloud.google.com/composer/environments/detail/test-location/test-env"
                    "/logs?project=test-project"
                ),
            ),
            ("Composer Documentation", "https://cloud.google.com/composer/docs"),
        ],
    )
    def test_menu_links(self, expected_label, expected_href):
        menu_items = composer_menu_plugin.ComposerMenuPlugin().appbuilder_menu_items

        assert expected_label in [menu_item["label"] for menu_item in menu_items]
        for menu_item in menu_items:
            if menu_item["label"] == expected_label:
                assert menu_item["href"] == expected_href

    def test_menu_items_under_same_category(self):
        for menu_item in composer_menu_plugin.ComposerMenuPlugin().appbuilder_menu_items:
            assert menu_item["category_label"] == "Composer"

    @mock.patch("airflow.composer.patches.webserver.composer_menu_plugin.register_plugin", autospec=True)
    def test_register_composer_menu_plugin(self, register_plugin_mock):
        register_composer_menu_plugin()

        assert len(register_plugin_mock.call_args_list) == 1
        actual_plugin = register_plugin_mock.call_args_list[0][0][0]
        assert isinstance(actual_plugin, composer_menu_plugin.ComposerMenuPlugin)
        assert isinstance(actual_plugin.source, composer_menu_plugin.ComposerMenuPluginSource)

    def test_composer_menu_plugin_source(self):
        source = composer_menu_plugin.ComposerMenuPluginSource()

        assert str(source) == "airflow.composer.patches.webserver.composer_menu_plugin"
        assert source.__html__() == "airflow.composer.patches.webserver.composer_menu_plugin"
