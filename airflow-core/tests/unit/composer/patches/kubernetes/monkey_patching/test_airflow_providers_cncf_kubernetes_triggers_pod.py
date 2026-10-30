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

import datetime
import itertools
import json
import types
from unittest import mock

import pytest
from kubernetes.client import models as k8s

from airflow.composer.patches.kubernetes.monkey_patching.airflow_providers_cncf_kubernetes_triggers_pod import (
    CONTAINER_STATUSES_NOT_FOUND_GRACE_PERIOD_SECONDS,
    _composer_kubernetes_pod_operator_write_logs,
    _composer_kubernetes_pod_trigger_define_container_state,
    patch,
)
from airflow.composer.patches.kubernetes.utils import (
    PEER_VM_ENDPOINT_ANNOTATION,
    PEER_VM_PLACEHOLDER_CONTAINER,
    PeerVmPodContainerStatusesNotFoundException,
    exec_on_placeholder_pod,
)
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.triggers.pod import ContainerState
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase

AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH = (
    "airflow.composer.patches.kubernetes.monkey_patching.airflow_providers_cncf_kubernetes_triggers_pod"
)
CONTAINER_STATUSES_NOT_FOUND_ERROR = (
    'There was an error in calling kubernetes API: E0811 23:26:57.282819 15 execclient.go:104] "could not '
    'call ContainerStatuses" err="rpc error: code = Unknown desc = failed to find containerStatuses in pod '
    'base in namespace composer-user-workloads" logger="ContainerStatuses"'
)


class TestAirflowProvidersCncfKubernetesTriggersPod:
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}._composer_kubernetes_pod_operator_write_logs",
    )
    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}._composer_kubernetes_pod_trigger_define_container_state",
    )
    def test_patch(
        self,
        _composer_kubernetes_pod_trigger_define_container_state_mock,
        _composer_kubernetes_pod_operator_write_logs_mock,
    ):
        _composer_kubernetes_pod_trigger_define_container_state_mock.assert_not_called()
        _composer_kubernetes_pod_operator_write_logs_mock.assert_not_called()

        patch()

        _composer_kubernetes_pod_trigger_define_container_state_mock.assert_called_once()
        _composer_kubernetes_pod_operator_write_logs_mock.assert_called_once()

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

    @pytest.fixture
    def peer_vm_trigger(self):
        """Call the patched define_container_state for a Peer VM pod, with container statuses mocked."""
        with (
            mock.patch(f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}.KubernetesHook"),
            mock.patch(
                f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}.PodManager"
            ) as pod_manager_mock,
            mock.patch(
                f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}.get_peer_vm_pod_container_statuses",
                autospec=True,
            ) as get_container_statuses_mock,
            mock.patch(
                f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}.time.monotonic",
                return_value=1000.0,
            ) as monotonic_mock,
        ):
            pod = k8s.V1Pod(
                spec=k8s.V1PodSpec(containers=[k8s.V1Container(name=PEER_VM_PLACEHOLDER_CONTAINER)]),
                metadata=k8s.V1ObjectMeta(
                    name="base",
                    namespace="default",
                    annotations={PEER_VM_ENDPOINT_ANNOTATION: "test_endpoint"},
                ),
                status=k8s.V1PodStatus(phase=PodPhase.RUNNING),
            )
            pod_manager_mock.return_value.read_pod.return_value = pod
            # A plain object, not a Mock, so we can check what the patch remembers between polls.
            trigger = types.SimpleNamespace(
                base_container_name="base",
                kubernetes_conn_id=None,
                in_cluster=True,
                config_dict=None,
                cluster_context=None,
                log=mock.Mock(),
            )
            define_container_state = _composer_kubernetes_pod_trigger_define_container_state(mock.Mock())

            def poll():
                return define_container_state(trigger, pod=pod)

            yield types.SimpleNamespace(
                poll=poll,
                trigger=trigger,
                get_container_statuses_mock=get_container_statuses_mock,
                monotonic_mock=monotonic_mock,
            )

    def test_define_container_state_statuses_not_found_after_container_observed(self, peer_vm_trigger):
        # The container runs, finishes, and Peer VM removes the pod before the next poll.
        peer_vm_trigger.get_container_statuses_mock.side_effect = [
            [{"container": "base", "state": "RUNNING"}],
            PeerVmPodContainerStatusesNotFoundException(CONTAINER_STATUSES_NOT_FOUND_ERROR),
        ]

        assert peer_vm_trigger.poll() == ContainerState.RUNNING
        assert peer_vm_trigger.poll() == ContainerState.TERMINATED

    def test_define_container_state_statuses_not_found_before_container_observed(self, peer_vm_trigger):
        # The pod isn't on Peer VM yet, so we should keep waiting.
        peer_vm_trigger.get_container_statuses_mock.side_effect = PeerVmPodContainerStatusesNotFoundException(
            CONTAINER_STATUSES_NOT_FOUND_ERROR
        )

        assert peer_vm_trigger.poll() == ContainerState.UNDEFINED

        peer_vm_trigger.monotonic_mock.return_value = (
            1000.0 + CONTAINER_STATUSES_NOT_FOUND_GRACE_PERIOD_SECONDS - 1
        )
        assert peer_vm_trigger.poll() == ContainerState.UNDEFINED

    def test_define_container_state_statuses_not_found_for_grace_period(self, peer_vm_trigger):
        # We never saw the container, e.g. it finished and was removed before the first poll.
        peer_vm_trigger.get_container_statuses_mock.side_effect = PeerVmPodContainerStatusesNotFoundException(
            CONTAINER_STATUSES_NOT_FOUND_ERROR
        )

        assert peer_vm_trigger.poll() == ContainerState.UNDEFINED

        peer_vm_trigger.monotonic_mock.return_value = (
            1000.0 + CONTAINER_STATUSES_NOT_FOUND_GRACE_PERIOD_SECONDS
        )
        assert peer_vm_trigger.poll() == ContainerState.TERMINATED

    def test_define_container_state_statuses_found_resets_grace_period(self, peer_vm_trigger):
        not_found = PeerVmPodContainerStatusesNotFoundException(CONTAINER_STATUSES_NOT_FOUND_ERROR)
        peer_vm_trigger.get_container_statuses_mock.side_effect = [
            not_found,
            [{"container": "base", "state": "WAITING"}],
            not_found,
        ]

        assert peer_vm_trigger.poll() == ContainerState.UNDEFINED

        peer_vm_trigger.monotonic_mock.return_value = 1030.0
        assert peer_vm_trigger.poll() == ContainerState.WAITING
        assert (
            getattr(peer_vm_trigger.trigger, "_composer_peer_vm_container_statuses_not_found_since") is None
        )

        # We've seen the container now, so if the pod disappears it has finished.
        assert peer_vm_trigger.poll() == ContainerState.TERMINATED

    def test_define_container_state_other_airflow_exception_is_raised(self, peer_vm_trigger):
        peer_vm_trigger.get_container_statuses_mock.side_effect = AirflowException("other error")

        with pytest.raises(AirflowException, match="other error"):
            peer_vm_trigger.poll()

    @pytest.fixture
    def peer_vm_exec_trigger(self, monkeypatch):
        """
        Call the patched define_container_state with only the Kubernetes exec stream mocked.

        Everything else is real (exec_on_placeholder_pod with its retries, get_peer_vm_pod_container_statuses),
        so the Peer VM error goes through the same code as in Composer.
        """
        monkeypatch.setattr(exec_on_placeholder_pod.retry, "sleep", mock.Mock())
        with (
            mock.patch(f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}.KubernetesHook"),
            mock.patch(
                f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}.PodManager"
            ) as pod_manager_mock,
            mock.patch(
                "airflow.composer.patches.kubernetes.utils.kubernetes_stream"
            ) as kubernetes_stream_mock,
        ):
            pod = k8s.V1Pod(
                spec=k8s.V1PodSpec(containers=[k8s.V1Container(name=PEER_VM_PLACEHOLDER_CONTAINER)]),
                metadata=k8s.V1ObjectMeta(
                    name="base",
                    namespace="composer-user-workloads",
                    annotations={PEER_VM_ENDPOINT_ANNOTATION: "test_endpoint"},
                ),
                status=k8s.V1PodStatus(phase=PodPhase.RUNNING),
            )
            pod_manager_mock.return_value.read_pod.return_value = pod
            trigger = types.SimpleNamespace(
                base_container_name="base",
                kubernetes_conn_id=None,
                in_cluster=True,
                config_dict=None,
                cluster_context=None,
                log=mock.Mock(),
            )
            define_container_state = _composer_kubernetes_pod_trigger_define_container_state(mock.Mock())

            yield types.SimpleNamespace(
                poll=lambda: define_container_state(trigger, pod=pod),
                kubernetes_stream_mock=kubernetes_stream_mock,
            )

    @staticmethod
    def _exec_stream_with_container_statuses(container_statuses):
        return mock.Mock(
            is_open=mock.Mock(return_value=True),
            peek_stdout=mock.Mock(side_effect=[True, False]),
            read_stdout=mock.Mock(return_value=json.dumps(container_statuses)),
            peek_stderr=mock.Mock(return_value=False),
            returncode=0,
        )

    @staticmethod
    def _exec_stream_with_container_statuses_not_found():
        # What the placeholder pod prints when Peer VM doesn't know the pod.
        return mock.Mock(
            is_open=mock.Mock(return_value=True),
            peek_stdout=mock.Mock(return_value=False),
            peek_stderr=mock.Mock(side_effect=itertools.cycle([True, False])),
            read_stderr=mock.Mock(
                return_value=CONTAINER_STATUSES_NOT_FOUND_ERROR.removeprefix(
                    "There was an error in calling kubernetes API: "
                )
            ),
        )

    def test_define_container_state_pod_reaped_on_peer_vm_after_container_finished(
        self, peer_vm_exec_trigger
    ):
        # The container runs, finishes, and Peer VM removes the pod before the next poll.
        peer_vm_exec_trigger.kubernetes_stream_mock.side_effect = [
            self._exec_stream_with_container_statuses([{"container": "base", "state": "RUNNING"}]),
            *(self._exec_stream_with_container_statuses_not_found() for _ in range(6)),
        ]

        assert peer_vm_exec_trigger.poll() == ContainerState.RUNNING
        # Before the fix, this raised AirflowException "... failed to find containerStatuses in pod ...".
        assert peer_vm_exec_trigger.poll() == ContainerState.TERMINATED
        # 1 successful call, then 6 attempts (with retries) that all got the error.
        assert peer_vm_exec_trigger.kubernetes_stream_mock.call_count == 7

    def test_define_container_state_pod_not_registered_on_peer_vm_yet(self, peer_vm_exec_trigger):
        # Peer VM doesn't know the pod before we ever saw the container, so we keep waiting.
        peer_vm_exec_trigger.kubernetes_stream_mock.side_effect = [
            *(self._exec_stream_with_container_statuses_not_found() for _ in range(6)),
            self._exec_stream_with_container_statuses([{"container": "base", "state": "RUNNING"}]),
        ]

        assert peer_vm_exec_trigger.poll() == ContainerState.UNDEFINED
        assert peer_vm_exec_trigger.poll() == ContainerState.RUNNING

    @mock.patch(
        f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}.PodManager",
    )
    @mock.patch(f"{AIRFLOW_PROVIDERS_CNCF_KUBERNETES_TRIGGERS_POD_MODULE_PATH}.write_logs_from_peer_vm")
    def test_composer_kubernetes_pod_operator_write_logs(
        self,
        write_logs_from_peer_vm_mock,
        pod_manager_mock,
    ):
        pod = k8s.V1Pod(
            spec=k8s.V1PodSpec(containers=[k8s.V1Container(name=PEER_VM_PLACEHOLDER_CONTAINER)]),
            metadata=k8s.V1ObjectMeta(
                name="base",
                namespace="default",
                annotations={PEER_VM_ENDPOINT_ANNOTATION: "test_endpoint"},
                creation_timestamp=datetime.datetime(2026, 6, 2, 15, 26, 38),
            ),
        )
        pod_manager_mock.return_value.read_pod.return_value = pod

        self_mock = mock.Mock(
            base_container_name="base",
            pod_manager=pod_manager_mock.return_value,
        )
        _composer_kubernetes_pod_operator_write_logs(mock.Mock())(
            self_mock,
            pod=pod,
        )

        write_logs_from_peer_vm_mock.assert_called_once_with(
            pod_manager_mock.return_value,
            container_name="base",
            peer_vm_endpoint="test_endpoint",
            after_timestamp="2026-06-02T15:26:38.0Z",
        )
