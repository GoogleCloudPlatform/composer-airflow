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

import json
import os
from unittest import mock

import pytest
from kubernetes.client import models as k8s
from kubernetes.client.exceptions import ApiException

from airflow.composer.patches.kubernetes.utils import (
    PeerVmPlaceholderPodContainerNotFoundException,
    PeerVmPlaceholderPodShutDownException,
    exec_on_placeholder_pod,
    get_peer_vm_pod_container_statuses,
    is_kubernetes_pod_operator_base_container_terminated,
    parse_payload_from_peer_vm_exec_response,
)
from airflow.exceptions import AirflowException


class TestUtils:
    @mock.patch("airflow.composer.patches.kubernetes.utils.kubernetes_stream", autospec=True)
    def test_exec_on_placeholder_pod(self, kubernetes_stream_mock):
        self_mock = mock.Mock()
        pod_mock = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="pod-name", namespace="pod-namespace"))
        kubernetes_stream_mock.return_value = mock.Mock(
            is_open=mock.Mock(return_value=True),
            peek_stdout=mock.Mock(side_effect=[True, False]),
            read_stdout=mock.Mock(return_value="content"),
            peek_stderr=mock.Mock(return_value=False),
            returncode=0,
        )

        result = exec_on_placeholder_pod(self_mock, pod=pod_mock, command=["arg1", "arg2"])

        kubernetes_stream_mock.assert_called_with(
            self_mock._client.connect_get_namespaced_pod_exec,
            "pod-name",
            "pod-namespace",
            container="peervm-placeholder",
            command=["arg1", "arg2"],
            stdin=True,
            stdout=True,
            stderr=True,
            tty=False,
            _preload_content=False,
        )
        assert result == "content"

    @mock.patch("airflow.composer.patches.kubernetes.utils.kubernetes_stream", autospec=True)
    def test_exec_on_placeholder_pod_error(self, kubernetes_stream_mock):
        self_mock = mock.Mock()
        pod_mock = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="pod-name", namespace="pod-namespace"))
        kubernetes_stream_mock.return_value = mock.Mock(
            is_open=mock.Mock(return_value=True),
            peek_stdout=mock.Mock(return_value=False),
            peek_stderr=mock.Mock(side_effect=[True, False]),
            read_stderr=mock.Mock(return_value="API error"),
        )

        with pytest.raises(AirflowException) as exc:
            exec_on_placeholder_pod(self_mock, pod=pod_mock, command=["arg1", "arg2"])

        kubernetes_stream_mock.assert_called_with(
            self_mock._client.connect_get_namespaced_pod_exec,
            "pod-name",
            "pod-namespace",
            container="peervm-placeholder",
            command=["arg1", "arg2"],
            stdin=True,
            stdout=True,
            stderr=True,
            tty=False,
            _preload_content=False,
        )
        assert str(exc.value) == "There was an error in calling kubernetes API: API error"

    @mock.patch("airflow.composer.patches.kubernetes.utils.kubernetes_stream", autospec=True)
    def test_exec_on_placeholder_pod_pod_shut_down(self, kubernetes_stream_mock):
        self_mock = mock.Mock()
        pod_mock = mock.Mock()
        kubernetes_stream_mock.return_value = mock.Mock(
            is_open=mock.Mock(return_value=False),
            peek_stdout=mock.Mock(return_value=False),
            peek_stderr=mock.Mock(return_value=False),
            returncode=137,
        )

        with pytest.raises(PeerVmPlaceholderPodShutDownException) as exc:
            exec_on_placeholder_pod(self_mock, pod=pod_mock, command=["arg1", "arg2"])

        assert str(exc.value) == "Got 137 exit code on exec"

    @mock.patch("airflow.composer.patches.kubernetes.utils.kubernetes_stream", autospec=True)
    def test_exec_on_placeholder_pod_unexpected_return_code(self, kubernetes_stream_mock):
        self_mock = mock.Mock()
        pod_mock = mock.Mock()
        kubernetes_stream_mock.return_value = mock.Mock(
            is_open=mock.Mock(return_value=False),
            peek_stdout=mock.Mock(return_value=False),
            peek_stderr=mock.Mock(return_value=False),
            returncode=58,
        )

        with pytest.raises(AirflowException) as exc:
            exec_on_placeholder_pod(self_mock, pod=pod_mock, command=["arg1", "arg2"])

        assert str(exc.value) == "There was an error in calling kubernetes API, return code: 58"

    @mock.patch("airflow.composer.patches.kubernetes.utils.kubernetes_stream", autospec=True)
    def test_exec_on_placeholder_pod_return_code_value_error(self, kubernetes_stream_mock):
        self_mock = mock.Mock()
        pod_mock = mock.Mock()

        class _KubernetesStreamMock(mock.Mock):
            def is_open(self):
                return False

            def peek_stdout(self):
                return False

            def peek_stderr(self):
                return False

            @property
            def returncode(self):
                raise ValueError("error")

        kubernetes_stream_mock.return_value = _KubernetesStreamMock()

        with pytest.raises(PeerVmPlaceholderPodShutDownException) as exc:
            exec_on_placeholder_pod(self_mock, pod=pod_mock, command=["arg1", "arg2"])

        assert str(exc.value) == "Error on parsing exit code"

    @mock.patch("airflow.composer.patches.kubernetes.utils.kubernetes_stream", autospec=True)
    def test_exec_on_placeholder_pod_container_not_found(self, kubernetes_stream_mock):
        self_mock = mock.Mock()
        pod_mock = mock.Mock()
        kubernetes_stream_mock.side_effect = ApiException(
            reason='Handshake status 500 Error -+-+- b\'container not found ("peervm-placeholder")'
        )

        with pytest.raises(PeerVmPlaceholderPodContainerNotFoundException) as exc:
            exec_on_placeholder_pod(self_mock, pod=pod_mock, command=["arg1", "arg2"])

        assert str(exc.value) == (
            'Handshake status 500 Error -+-+- b\'container not found ("peervm-placeholder")'
        )

    @mock.patch("airflow.composer.patches.kubernetes.utils.kubernetes_stream", autospec=True)
    def test_exec_on_placeholder_pod_agent_failed(self, kubernetes_stream_mock):
        self_mock = mock.Mock()
        pod_mock = mock.Mock()
        kubernetes_stream_mock.side_effect = ApiException(reason="Kubelet agent failed")

        with pytest.raises(ApiException) as exc:
            exec_on_placeholder_pod(self_mock, pod=pod_mock, command=["arg1", "arg2"])

        assert exc.value.reason == "Kubelet agent failed"

    @mock.patch("airflow.composer.patches.kubernetes.utils.exec_on_placeholder_pod", autospec=True)
    def test_get_peer_vm_pod_container_statuses(self, exec_on_placeholder_pod_mock):
        self_mock = mock.Mock()
        pod_mock = mock.Mock()

        def exec_on_placeholder_pod_mock_side_effect(self, pod=None, command=None):
            assert self == self_mock
            assert pod == pod_mock
            assert command == ["placeholder-pod", "container-statuses"]
            return '{"key": "value"}'

        exec_on_placeholder_pod_mock.side_effect = exec_on_placeholder_pod_mock_side_effect

        result = get_peer_vm_pod_container_statuses(self_mock, pod_mock)

        exec_on_placeholder_pod_mock.assert_called_once()
        assert result == {"key": "value"}

    @pytest.mark.parametrize(
        "container_statuses, expected_result",
        [
            (
                [
                    {"container": "airflow-xcom-sidecar", "state": "RUNNING"},
                    {"container": "base", "state": "TERMINATED"},
                ],
                True,
            ),
            (
                [
                    {"container": "airflow-xcom-sidecar", "state": "RUNNING"},
                    {"container": "base", "state": "RUNNING"},
                ],
                False,
            ),
        ],
    )
    @mock.patch("airflow.composer.patches.kubernetes.utils.get_peer_vm_pod_container_statuses", autospec=True)
    def test_is_kubernetes_pod_operator_base_container_terminated(
        self, get_peer_vm_pod_container_statuses_mock, container_statuses, expected_result
    ):
        get_peer_vm_pod_container_statuses_mock.return_value = container_statuses
        self_mock = mock.Mock()
        pod_mock = mock.Mock()

        actual_result = is_kubernetes_pod_operator_base_container_terminated(self_mock, pod_mock)

        get_peer_vm_pod_container_statuses_mock.assert_called_with(self_mock, pod=pod_mock)
        assert actual_result == expected_result

    @mock.patch("airflow.composer.patches.kubernetes.utils.get_peer_vm_pod_container_statuses", autospec=True)
    def test_is_kubernetes_pod_operator_base_container_terminated_unexpected_list_of_containers(
        self, get_peer_vm_pod_container_statuses_mock
    ):
        get_peer_vm_pod_container_statuses_mock.return_value = [
            {"container": "airflow-xcom-sidecar", "state": "RUNNING"}
        ]

        self_mock = mock.Mock()
        pod_mock = mock.Mock()

        with pytest.raises(ValueError) as exc:
            is_kubernetes_pod_operator_base_container_terminated(self_mock, pod_mock)

        get_peer_vm_pod_container_statuses_mock.assert_called_with(self_mock, pod=pod_mock)
        assert str(exc.value) == (
            "Unexpected list of containers in KubernetesPodOperator pod, container statuses: "
            "[{'container': 'airflow-xcom-sidecar', 'state': 'RUNNING'}]"
        )

    @pytest.mark.parametrize(
        "response, expected_result",
        [
            (b"ggEBghMBeyJrZXk6IjogInZhbHVlIn0KiAID6A==", '{"key:": "value"}\n'),
            (b"ggEBggUBMjIyCogCA+g=", "222\n"),
        ],
    )
    def test_parse_payload_from_peer_vm_exec_response(self, response, expected_result):
        actual_result = parse_payload_from_peer_vm_exec_response(response)

        assert actual_result == expected_result

    def test_parse_payload_from_peer_vm_exec_response_big_payload_many_frames(self):
        peer_vm_exec_response_100000_length_list = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "test_data/peer_vm_exec_response_100000_length_list",
        )
        with open(peer_vm_exec_response_100000_length_list, "rb") as f:
            response = f.read()

        result = parse_payload_from_peer_vm_exec_response(response)

        assert result == (json.dumps(list(range(100000))) + "\n")

    def test_parse_payload_from_peer_vm_exec_response_no_stdout_stderr_frames(self):
        actual_result = parse_payload_from_peer_vm_exec_response(b"ggEBiAID6A==")

        assert actual_result == ""

    def test_parse_payload_from_peer_vm_exec_response_broken_frame(self):
        with pytest.raises(AirflowException) as exc:
            parse_payload_from_peer_vm_exec_response(b"fgEBiAID6A==")

        assert str(exc.value) == "Unable to parse response from exec command: invalid opcode"
