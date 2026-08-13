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
from kubernetes.client import models as k8s

from airflow.composer.patches.kubernetes.monkey_patching.airflow_providers_cncf_kubernetes_triggers_pod import (
    _composer_kubernetes_pod_trigger_define_container_state,
    patch,
)
from airflow.composer.patches.kubernetes.utils import (
    PEER_VM_ENDPOINT_ANNOTATION,
    PEER_VM_PLACEHOLDER_CONTAINER,
)
from airflow.providers.cncf.kubernetes.triggers.pod import ContainerState
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase

AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH = (
    "airflow.composer.patches.kubernetes.monkey_patching.airflow_providers_cncf_kubernetes_triggers_pod"
)


class TestAirflowProvidersCncfKubernetesTriggersPod:
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}._composer_kubernetes_pod_trigger_define_container_state",
    )
    def test_patch(self, _composer_kubernetes_pod_trigger_define_container_state_mock):
        _composer_kubernetes_pod_trigger_define_container_state_mock.assert_not_called()

        patch()

        _composer_kubernetes_pod_trigger_define_container_state_mock.assert_called_once()

    @pytest.mark.parametrize(
        ("container_statuses", "expected_state", "pod_phase"),
        [
            (
                [
                    {"container": "airflow-xcom-sidecar", "state": "RUNNING"},
                    {"container": "base", "state": "TERMINATED"},
                ],
                ContainerState.TERMINATED,
                PodPhase.RUNNING,
            ),
            (
                [
                    {"container": "airflow-xcom-sidecar", "state": "RUNNING"},
                    {"container": "base", "state": "RUNNING"},
                ],
                ContainerState.RUNNING,
                PodPhase.RUNNING,
            ),
            (
                [],
                ContainerState.UNDEFINED,
                PodPhase.PENDING,
            ),
        ],
    )
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}.get_peer_vm_pod_container_statuses",
        autospec=True,
    )
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}.PodManager",
    )
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}.KubernetesHook",
    )
    def test_composer_kubernetes_pod_trigger_define_container_state(
        self,
        k8s_hook_mock,
        pod_manager_mock,
        get_peer_vm_pod_container_statuses_mock,
        container_statuses,
        expected_state,
        pod_phase,
    ):
        pod = k8s.V1Pod(
            spec=k8s.V1PodSpec(containers=[k8s.V1Container(name=PEER_VM_PLACEHOLDER_CONTAINER)]),
            metadata=k8s.V1ObjectMeta(
                name="base", namespace="default", annotations={PEER_VM_ENDPOINT_ANNOTATION: "test_endpoint"}
            ),
            status=k8s.V1PodStatus(phase=pod_phase),
        )
        pod_manager_mock.return_value.read_pod.return_value = pod
        get_peer_vm_pod_container_statuses_mock.return_value = container_statuses

        self_mock = mock.Mock(base_container_name="base")
        state_result = _composer_kubernetes_pod_trigger_define_container_state(mock.Mock())(
            self_mock, pod=pod
        )

        assert expected_state == state_result
