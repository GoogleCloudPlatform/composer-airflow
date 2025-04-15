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

import pytest

import airflow.utils.net
from airflow.composer.patches.core.utils import (
    apply_monkey_patching_patches,
    get_component_hostname,
    get_composer_gke_cluster_host,
    is_currently_running_component,
    is_triggerer_enabled,
)

from tests_common.test_utils.config import conf_vars


class TestUtils:
    def test_is_triggerer_enabled_default(self):
        assert is_triggerer_enabled() is False

    @conf_vars({("composer_internal", "enable_triggerer"): "True"})
    def test_is_triggerer_enabled_true(self):
        assert is_triggerer_enabled() is True

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

    @conf_vars({("kubernetes_executor", "config_file"): "/test_kube_config_file"})
    @mock.patch("airflow.composer.patches.core.utils.config", autospec=True)
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
        "sys_argv, component_name, expected_result",
        [
            (["airflow", "triggerer"], "triggerer", True),
            (["airflow", "scheduler"], "triggerer", False),
            (["airflow"], "triggerer", False),
            ([], "triggerer", False),
            (["airflow", "scheduler"], "scheduler", True),
        ],
    )
    def test_is_currently_running_component(self, sys_argv, component_name, expected_result):
        with mock.patch("sys.argv", sys_argv):
            assert is_currently_running_component(component_name) is expected_result

    @mock.patch(
        "airflow.composer.patches.core.utils.COMPOSER_PATCHES_PACKAGE",
        "unit.composer.patches.core.test_data.cross_composer_patches_method",
    )
    @mock.patch(
        "airflow.composer.patches.core.utils.COMPOSER_PATCHES_PACKAGE_PATH",
        "/composer-airflow/airflow-core/tests/unit/composer/patches/core/test_data/cross_composer_patches_method/",
    )
    @mock.patch("unit.composer.patches.core.test_data.cross_composer_patches_method.hook", autospec=True)
    def test_cross_composer_patches_method(self, hook_mock):
        from unit.composer.patches.core.test_data.cross_composer_patches_method.core.module_a import method_a

        method_a("prefix")

        assert hook_mock.call_args_list == [
            mock.call("prefix patch core"),
            mock.call("prefix patch bear"),
            mock.call("prefix patch dog"),
        ]

    @mock.patch(
        "airflow.composer.patches.core.utils.COMPOSER_PATCHES_PACKAGE",
        "unit.composer.patches.core.test_data.apply_monkey_patching_patches",
    )
    @mock.patch(
        "airflow.composer.patches.core.utils.COMPOSER_PATCHES_PACKAGE_PATH",
        "/composer-airflow/airflow-core/tests/unit/composer/patches/core/test_data/apply_monkey_patching_patches/",
    )
    @mock.patch(
        "unit.composer.patches.core.test_data.apply_monkey_patching_patches.core.monkey_patching.module_a.patch",
        autospec=True,
    )
    def test_apply_monkey_patching_patches(self, module_a_patch_mock):
        apply_monkey_patching_patches()

        module_a_patch_mock.assert_called_once_with()
