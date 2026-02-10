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

from airflow.composer.patches.kubernetes.monkey_patching.airflow_providers_cncf_kubernetes_executors_kubernetes_executor import (
    _composer_kubernetes_executor_start,
    _composer_kubernetes_executor_sync,
    patch,
)

AIRFLOW_PROVIDERS_CNCF_KUBERNETES_EXECUTORS_KUBERNETES_EXECUTOR_MODULE_PATH = "airflow.composer.patches.kubernetes.monkey_patching.airflow_providers_cncf_kubernetes_executors_kubernetes_executor"


class TestAirflowProvidersCncfKubernetesExecutorsKubernetesExecutor:
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_EXECUTORS_KUBERNETES_EXECUTOR_MODULE_PATH}._composer_kubernetes_executor_start",
    )
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_EXECUTORS_KUBERNETES_EXECUTOR_MODULE_PATH}._composer_kubernetes_executor_sync",
    )
    def test_patch(self, composer_kubernetes_executor_sync_mock, composer_kubernetes_executor_start_mock):
        composer_kubernetes_executor_start_mock.assert_not_called()
        composer_kubernetes_executor_sync_mock.assert_not_called()

        patch()

        composer_kubernetes_executor_start_mock.assert_called_once()
        composer_kubernetes_executor_sync_mock.assert_called_once()

    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_EXECUTORS_KUBERNETES_EXECUTOR_MODULE_PATH}.refresh_pod_template_file"
    )
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_EXECUTORS_KUBERNETES_EXECUTOR_MODULE_PATH}.time.time",
        return_value=34,
    )
    def test_composer_kubernetes_executor_start(self, time_mock, refresh_pod_template_file_mock):
        self_mock = mock.Mock()
        api_client_mock = mock.Mock()
        self_mock.kube_client.api_client = api_client_mock

        actual_result = _composer_kubernetes_executor_start(lambda self, arg, kwarg: arg + kwarg)(
            self_mock, 1, kwarg=2
        )

        assert self_mock._composer_pod_template_file_timestamp == 34
        refresh_pod_template_file_mock.assert_called_once_with(api_client_mock)
        assert actual_result == 3

    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_EXECUTORS_KUBERNETES_EXECUTOR_MODULE_PATH}.refresh_pod_template_file"
    )
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_EXECUTORS_KUBERNETES_EXECUTOR_MODULE_PATH}.time.time",
        return_value=100,
    )
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_EXECUTORS_KUBERNETES_EXECUTOR_MODULE_PATH}.POD_TEMPLATE_FILE_REFRESH_INTERVAL",
        50,
    )
    def test_composer_kubernetes_executor_sync(self, time_mock, refresh_pod_template_file_mock):
        self_mock = mock.Mock()
        api_client_mock = mock.Mock()
        self_mock.kube_client.api_client = api_client_mock
        self_mock._composer_pod_template_file_timestamp = 40

        actual_result = _composer_kubernetes_executor_sync(lambda self, arg, kwarg: arg + kwarg)(
            self_mock, 3, kwarg=4
        )

        assert self_mock._composer_pod_template_file_timestamp == 100
        refresh_pod_template_file_mock.assert_called_once_with(api_client_mock)
        assert actual_result == 7

    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_EXECUTORS_KUBERNETES_EXECUTOR_MODULE_PATH}.refresh_pod_template_file"
    )
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_EXECUTORS_KUBERNETES_EXECUTOR_MODULE_PATH}.time.time",
        return_value=100,
    )
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_EXECUTORS_KUBERNETES_EXECUTOR_MODULE_PATH}.POD_TEMPLATE_FILE_REFRESH_INTERVAL",
        50,
    )
    def test_composer_kubernetes_executor_sync_should_not_refresh(
        self, time_mock, refresh_pod_template_file_mock
    ):
        self_mock = mock.Mock()
        api_client_mock = mock.Mock()
        self_mock.kube_client.api_client = api_client_mock
        self_mock._composer_pod_template_file_timestamp = 70

        actual_result = _composer_kubernetes_executor_sync(lambda self, arg, kwarg: arg + kwarg)(
            self_mock, 3, kwarg=4
        )

        assert self_mock._composer_pod_template_file_timestamp == 70
        refresh_pod_template_file_mock.assert_not_called()
        assert actual_result == 7
