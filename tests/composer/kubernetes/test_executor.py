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

import os
from unittest import mock

import yaml
from kubernetes.client import (
    ApiClient,
    V1Affinity,
    V1Container,
    V1Deployment,
    V1DeploymentSpec,
    V1EnvVar,
    V1NodeAffinity,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
    V1PreferredSchedulingTerm,
    V1Probe,
)

from airflow.composer.kubernetes.executor import (
    POD_TEMPLATE_FILE,
    _composer_kubernetes_executor_init,
    _composer_kubernetes_executor_start,
    _composer_kubernetes_executor_sync,
    get_task_run_command_from_args,
    patch_kubernetes_executor,
    refresh_pod_template_file,
)
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor


class TestExecutor:
    @mock.patch("airflow.composer.kubernetes.executor.COMPOSER_VERSIONED_NAMESPACE", "test-namespace")
    @mock.patch("airflow.composer.kubernetes.executor.AppsV1Api", autospec=True)
    @mock.patch.dict("os.environ", {"COMPOSER_VERSION": "1.18.0"})
    def test_refresh_pod_template_file_composer_v1(self, mock_apps_v1_api_class):
        mock_api_client = ApiClient()
        mock_kube_client = mock.MagicMock()

        def read_namespaced_deployment_side_effect(name, namespace):
            assert name == "airflow-worker"
            assert namespace == "test-namespace"
            return V1Deployment(
                spec=V1DeploymentSpec(
                    selector={},
                    template=V1PodTemplateSpec(
                        metadata=V1ObjectMeta(labels={"label1": "value1"}),
                        spec=V1PodSpec(
                            affinity=V1Affinity(
                                node_affinity=V1NodeAffinity(
                                    preferred_during_scheduling_ignored_during_execution=V1PreferredSchedulingTerm(  # noqa: E501
                                        preference={},
                                        weight=100,
                                    ),
                                )
                            ),
                            containers=[
                                V1Container(
                                    name="test-container",
                                    liveness_probe=V1Probe(failure_threshold=2),
                                    env=[V1EnvVar(name="env1", value="value1")],
                                ),
                                V1Container(
                                    name="sidecar",
                                ),
                            ],
                            restart_policy="Always",
                        ),
                    ),
                )
            )

        mock_kube_client.read_namespaced_deployment.side_effect = read_namespaced_deployment_side_effect

        def mock_apps_v1_api_class_side_effect(api_client):
            assert api_client == mock_api_client
            return mock_kube_client

        mock_apps_v1_api_class.side_effect = mock_apps_v1_api_class_side_effect

        if os.path.exists(POD_TEMPLATE_FILE):
            os.remove(POD_TEMPLATE_FILE)
        assert os.path.exists(POD_TEMPLATE_FILE) is False

        refresh_pod_template_file(mock_api_client)

        assert os.path.exists(POD_TEMPLATE_FILE) is True
        with open(POD_TEMPLATE_FILE) as f:
            expected_pod_template_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "test_refresh_pod_template_file_composer_v1.yaml"
            )
            with open(expected_pod_template_file) as f_expected:
                assert yaml.safe_load(f_expected.read()) == yaml.safe_load(f.read())

    @mock.patch("airflow.composer.kubernetes.executor.COMPOSER_VERSIONED_NAMESPACE", "test-namespace")
    @mock.patch("airflow.composer.kubernetes.executor.CustomObjectsApi", autospec=True)
    @mock.patch.dict("os.environ", {"COMPOSER_VERSION": "2.0.0"})
    def test_refresh_pod_template_file_composer_v2(self, mock_custom_objects_api_class):
        mock_api_client = ApiClient()
        mock_kube_client = mock.MagicMock()

        def get_namespaced_custom_object_side_effect(group, version, plural, name, namespace):
            assert group == "composer.cloud.google.com"
            assert version == "v1beta1"
            assert plural == "airflowworkersets"
            assert name == "airflow-worker"
            assert namespace == "test-namespace"
            return {
                "spec": {
                    "template": {
                        "metadata": {},
                        "spec": {
                            "containers": [
                                {
                                    "name": "test-container",
                                    "env": [
                                        {
                                            "name": "env1",
                                            "value": "value1",
                                        }
                                    ],
                                    "livenessProbe": {},
                                },
                                {
                                    "name": "sidecar",
                                },
                            ]
                        },
                    }
                }
            }

        mock_kube_client.get_namespaced_custom_object.side_effect = get_namespaced_custom_object_side_effect

        def mock_custom_objects_api_class_side_effect(api_client):
            assert api_client == mock_api_client
            return mock_kube_client

        mock_custom_objects_api_class.side_effect = mock_custom_objects_api_class_side_effect

        if os.path.exists(POD_TEMPLATE_FILE):
            os.remove(POD_TEMPLATE_FILE)
        assert os.path.exists(POD_TEMPLATE_FILE) is False

        refresh_pod_template_file(mock_api_client)

        assert os.path.exists(POD_TEMPLATE_FILE) is True
        with open(POD_TEMPLATE_FILE) as f:
            expected_pod_template_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "test_refresh_pod_template_file_composer_v2.yaml"
            )
            with open(expected_pod_template_file) as f_expected:
                assert yaml.safe_load(f_expected.read()) == yaml.safe_load(f.read())

    def test_get_task_run_command_from_args(self):
        assert (
            get_task_run_command_from_args(["airflow", "tasks", "run", "dag'id"])
            == "'airflow' 'tasks' 'run' 'dag'\\''id'"
        )

    @mock.patch("airflow.composer.kubernetes.executor._composer_kubernetes_executor_init", autospec=True)
    @mock.patch("airflow.composer.kubernetes.executor._composer_kubernetes_executor_start", autospec=True)
    def test_patch_fetch_container_logs(
        self, _composer_patch_kubernetes_executor_start_mock, _composer_patch_kubernetes_executor_init_mock,
    ):
        # test setUp
        KubernetesExecutor.start._composer_patched = False

        # Call twice to check patching occurres only once.
        patch_kubernetes_executor()
        patch_kubernetes_executor()

        _composer_patch_kubernetes_executor_start_mock.assert_called_once()
        _composer_patch_kubernetes_executor_init_mock.assert_called_once()
        assert getattr(KubernetesExecutor.start, "_composer_patched") is True

    @mock.patch("airflow.composer.kubernetes.executor.EventScheduler", autospec=True)
    @mock.patch("airflow.composer.kubernetes.executor._composer_kubernetes_executor_sync", autospec=True)
    def test_patch_kubernetes_executor_init_and_sync_without_event_scheduler(
        self, _composer_patch_kubernetes_executor_sync_mock, event_scheduler_mock
    ):
        _composer_kubernetes_executor_init(lambda _: None)(mock.Mock(spec=KubernetesExecutor))

        event_scheduler_mock.assert_called_once()
        _composer_patch_kubernetes_executor_sync_mock.assert_called_once()

    @mock.patch("airflow.composer.kubernetes.executor.EventScheduler", autospec=True)
    @mock.patch("airflow.composer.kubernetes.executor._composer_kubernetes_executor_sync", autospec=True)
    def test_patch_kubernetes_executor_init_and_sync_with_event_scheduler(
        self, _composer_patch_kubernetes_executor_sync_mock, event_scheduler_mock
    ):
        _composer_kubernetes_executor_init(lambda _: None)(mock.Mock(event_scheduler=mock.Mock()))

        event_scheduler_mock.assert_not_called()
        _composer_patch_kubernetes_executor_sync_mock.assert_not_called()

    @mock.patch("airflow.composer.kubernetes.executor.EventScheduler", autospec=True)
    def test_composer_kubernetes_executor_sync(
        self, event_scheduler_mock
    ):
        mocked_executor = mock.Mock(event_scheduler=event_scheduler_mock)
        mocked_executor.sync.return_value = "test_value"
        mocked_executor.sync = _composer_kubernetes_executor_sync(mocked_executor.sync)(mocked_executor)

        assert mocked_executor.sync == "test_value"
        event_scheduler_mock.run.assert_called_with(blocking=False)

    @mock.patch("airflow.composer.kubernetes.executor.refresh_pod_template_file", autospec=True)
    def test_composer_get_container_names(self, refresh_pod_template_file_mock):
        _composer_kubernetes_executor_start(lambda _: None)(mock.Mock())

        refresh_pod_template_file_mock.assert_called_once()
