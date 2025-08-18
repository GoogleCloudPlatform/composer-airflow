#
# Copyright 2021 Google LLC
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

import pytest

import airflow.utils.net
from airflow import settings
from airflow.composer.data_lineage.utils import xcom_pull
from airflow.composer.utils import (
    COMPOSER_DEFAULT_CELERY_CONFIG,
    _is_triggerer_launch_command,
    get_component_hostname,
    get_composer_gke_cluster_host,
    get_composer_version,
    get_locational_endpoint,
    initialize,
    is_composer_v1,
    is_serverless_composer,
    is_triggerer_enabled,
)
from airflow.providers.celery.executors.default_celery import DEFAULT_CELERY_CONFIG
from tests.test_utils.config import conf_vars


class TestUtils:
    @mock.patch.dict("os.environ", COMPOSER_VERSION="1.16.6")
    def test_get_composer_version(self):
        assert get_composer_version() == "1.16.6"

    def test_is_composer_v1(self):
        with mock.patch.dict("os.environ", COMPOSER_VERSION="1.16.6"):
            assert is_composer_v1() is True

        with mock.patch.dict("os.environ", COMPOSER_VERSION="2.0.0-preview.0"):
            assert is_composer_v1() is False

        with mock.patch.dict("os.environ", clear=True):
            assert is_composer_v1() is False

    def test_is_triggerer_enabled_default(self):
        assert is_triggerer_enabled() is False

    @pytest.mark.parametrize(
        "composer_version, expected_result",
        [
            ("", False),
            ("1.20.12", False),
            ("2.0.0", False),
            ("2.4.21", False),
            ("2.50.0", False),
            ("2.50.0-preview.0", False),
            ("2.50.0-preview.1", False),
            ("2.65.0", False),
            ("3.0.0-preview.0", True),
            ("3.0.0", True),
            ("10.0.0", True),
        ],
    )
    def test_is_serverless_composer(self, composer_version, expected_result):
        with mock.patch.dict("os.environ", {"COMPOSER_VERSION": composer_version}):
            assert is_serverless_composer() == expected_result

    @mock.patch("airflow.composer.utils.initialize", autospec=True)
    def test_initialize(self, initialize_mock):
        settings.initialize()

        initialize_mock.assert_called_once()

    @conf_vars({("kubernetes_executor", "config_file"): "/test_kube_config_file"})
    @mock.patch("airflow.composer.utils.config", autospec=True)
    def test_get_composer_gke_cluster_host(self, config_mock):
        def load_kube_config_side_effect(config_file, client_configuration, persist_config):
            assert config_file == "/test_kube_config_file"
            assert persist_config is False
            client_configuration.host = "http://test-host-cluster"

        config_mock.load_kube_config.side_effect = load_kube_config_side_effect

        # Call twice to test cache.
        host1 = get_composer_gke_cluster_host()
        host2 = get_composer_gke_cluster_host()

        assert host1 == "http://test-host-cluster"
        assert host2 == "http://test-host-cluster"
        config_mock.load_kube_config.assert_called_once()

    @pytest.mark.parametrize(
        "hostname, expected_result",
        [
            ("airflow-worker-123", "airflow-worker-123"),
            ("airflow-worker-123.internal", "airflow-worker-123"),
            ("airflow-worker-123.internal.domain", "airflow-worker-123.internal.domain"),
        ],
    )
    @mock.patch.object(airflow.utils.net, "getfqdn", autospec=True)
    def test_get_component_hostname(self, getfqdn_mock, hostname, expected_result):
        getfqdn_mock.return_value = hostname

        assert get_component_hostname() == expected_result

    def test_composer_default_celery_config(self):
        assert DEFAULT_CELERY_CONFIG.items() <= COMPOSER_DEFAULT_CELERY_CONFIG.items()
        assert "redis_backend_health_check_interval" in COMPOSER_DEFAULT_CELERY_CONFIG
        assert COMPOSER_DEFAULT_CELERY_CONFIG["redis_backend_health_check_interval"] == 30

    @mock.patch("aiodebug.log_slow_callbacks", autospec=True)
    @mock.patch("sys.argv", ["/opt/python3.11/bin/airflow", "triggerer"])
    def test_is_aiodebug_called(self, aiodebug_log_slow_callbacks_mock):
        initialize()

        aiodebug_log_slow_callbacks_mock.enable.assert_called_once()

    def test_xcom_pull(self):
        test_key = "test_key"
        test_task_id = "test_task_id"
        test_map_index = -1

        mock_task_instance = mock.MagicMock()
        mock_task_instance.task_id = test_task_id
        mock_task_instance.map_index = test_map_index
        expected = mock_task_instance.xcom_pull.return_value

        actual = xcom_pull(task_instance=mock_task_instance, key=test_key)

        assert actual == expected
        mock_task_instance.xcom_pull.assert_called_once_with(task_ids=test_task_id, key=test_key)

    def test_xcom_pull_mapped_index(self):
        test_key = "test_key"
        test_task_id = "test_task_id"
        test_map_index = 1

        mock_task_instance = mock.MagicMock()
        mock_task_instance.task_id = test_task_id
        mock_task_instance.map_index = test_map_index
        expected = mock.MagicMock()
        mock_task_instance.xcom_pull.return_value = [mock.MagicMock(), expected, mock.MagicMock()]

        actual = xcom_pull(task_instance=mock_task_instance, key=test_key)

        assert actual == expected
        mock_task_instance.xcom_pull.assert_called_once_with(task_ids=test_task_id, key=test_key)

    @pytest.mark.parametrize(
        "service, location, version, response_ok, expected_value",
        [
            ("service", "location", "version", True, "location-service.googleapis.com"),
            ("service", "location", "version", False, None),
        ],
    )
    def test_get_locational_endpoint(self, service, location, version, response_ok, expected_value):
        mock_response = mock.MagicMock()
        mock_response.ok = response_ok

        with mock.patch("requests.get", return_value=mock_response) as mock_get:
            result = get_locational_endpoint(service, location, version)

            mock_get.assert_called_once()
            assert result == expected_value

    @pytest.mark.parametrize(
        "sys_argv_command, expected_result",
        [
            (["airflow", "triggerer"], True),
            (["triggerer"], False),
            (["airflow", "worker"], False),
            (["", ""], False),
        ],
    )
    def test_is_triggerer_cmd_passed(self, sys_argv_command, expected_result):
        with mock.patch("sys.argv", ["/opt/python3.11/bin/airflow", "triggerer"]):
            assert _is_triggerer_launch_command(sys_argv_command) == expected_result

    @pytest.mark.parametrize(
        "composer_version, patch_function_expected_calls_count",
        [
            ("2.1.10", 0),
            ("3.0.1", 1),
        ],
    )
    @mock.patch("sys.argv", ["/opt/python3.11/bin/airflow", "triggerer"])
    @mock.patch("airflow.composer.kubernetes.trigger.patch_kubernetes_hook")
    @mock.patch("airflow.composer.kubernetes.trigger.patch_define_container_state")
    def test_is_kpo_deferrable_patched(
        self,
        mock_container_state,
        kubernetes_hook_patch_mock,
        composer_version,
        patch_function_expected_calls_count,
    ):
        with mock.patch.dict("os.environ", {"COMPOSER_VERSION": composer_version}):
            initialize()

        assert mock_container_state.call_count == patch_function_expected_calls_count
        assert kubernetes_hook_patch_mock.call_count == patch_function_expected_calls_count
