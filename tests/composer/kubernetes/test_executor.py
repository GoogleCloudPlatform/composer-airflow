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
import os
import unittest
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
    get_task_run_command_from_args,
    refresh_pod_template_file,
)


class TestExecutor(unittest.TestCase):
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
