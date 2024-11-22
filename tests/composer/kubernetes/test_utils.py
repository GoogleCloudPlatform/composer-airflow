from __future__ import annotations

import os
from unittest import mock

import json
import pytest
import yaml
from kubernetes.client import models as k8s

from airflow.composer.kubernetes.utils import (
    _get_composer_serverless_machine_type,
    _get_composer_serverless_pod_metadata,
    exec_on_placeholder_pod,
    get_peer_vm_pod_container_statuses,
    is_kubernetes_pod_operator_base_container_terminated,
    parse_payload_from_peer_vm_exec_response,
    pod_mutation_hook_composer_serverless,
)
from airflow.exceptions import AirflowException


class TestUtils:
    @mock.patch.dict("os.environ", {"GCP_TENANT_PROJECT": "test-project-234"})
    def test_get_composer_serverless_pod_metadata(self):
        actual = _get_composer_serverless_pod_metadata(
            pod=k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]))
        )

        assert actual == k8s.V1ObjectMeta(
            namespace="composer-user-workloads",
            labels={"node.gke.io/use-workload-identity-service": "true"},
            annotations={
                "node.gke.io/gce-vm": yaml.dump(
                    {
                        "selector": {
                            "matchLabels": {
                                "machineType": "e2-custom-small-2048",
                                "diskSizeGb": "30",
                            },
                        },
                        "logging": ["Workload", "System"],
                        "vmServiceAccount": "peervm-vm@test-project-234.iam.gserviceaccount.com",
                    }
                ),
            },
        )

    @pytest.mark.parametrize(
        "pod, expected_disk_size_gb",
        [
            (
                k8s.V1Pod(
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                resources=k8s.V1ResourceRequirements(
                                    requests={"ephemeral-storage": "15G"}, limits={"ephemeral-storage": "20G"}
                                ),
                            )
                        ]
                    )
                ),
                "50",
            ),
            (
                k8s.V1Pod(
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                resources=k8s.V1ResourceRequirements(
                                    requests={"ephemeral-storage": "17G"}, limits={"ephemeral-storage": "15G"}
                                ),
                            )
                        ]
                    )
                ),
                "47",
            ),
            (
                k8s.V1Pod(
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                resources=k8s.V1ResourceRequirements(
                                    requests={"wrong-parameter": "17G"}, limits={"wrong-parameter": "15G"}
                                ),
                            )
                        ]
                    )
                ),
                "30",
            ),
            (
                k8s.V1Pod(
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                resources=k8s.V1ResourceRequirements(
                                    requests={"ephemeral-storage": "17Gi"},
                                    limits={"ephemeral-storage": "15Gi"},
                                ),
                            )
                        ]
                    )
                ),
                "49",
            ),
            (
                k8s.V1Pod(
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                resources=k8s.V1ResourceRequirements(
                                    requests={"ephemeral-storage": "15Gi"},
                                    limits={"ephemeral-storage": "17Gi"},
                                ),
                            )
                        ]
                    )
                ),
                "49",
            ),
            (
                k8s.V1Pod(
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                resources=k8s.V1ResourceRequirements(limits={"ephemeral-storage": "11G"}),
                            )
                        ]
                    )
                ),
                "41",
            ),
            (
                k8s.V1Pod(
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                resources=k8s.V1ResourceRequirements(requests={"ephemeral-storage": "12G"}),
                            )
                        ]
                    )
                ),
                "42",
            ),
            (
                k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")])),
                "30",
            ),
            (
                k8s.V1Pod(
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                resources=k8s.V1ResourceRequirements(requests={"ephemeral-storage": "200G"}),
                            )
                        ]
                    )
                ),
                "130",
            ),
        ],
    )
    @mock.patch.dict("os.environ", {"COMPOSER_GKE_LOCATION": "us-east1"})
    @mock.patch.dict("os.environ", {"GCP_TENANT_PROJECT": "test-project-234"})
    def test_get_composer_serverless_pod_metadata_disk_size_gb(self, pod, expected_disk_size_gb):
        actual_pod_metadata = _get_composer_serverless_pod_metadata(pod)

        actual_disk_size_gb = yaml.safe_load(actual_pod_metadata.annotations.get("node.gke.io/gce-vm"))[
            "selector"
        ]["matchLabels"]["diskSizeGb"]
        assert actual_disk_size_gb == expected_disk_size_gb

    @mock.patch.dict("os.environ", {"COMPOSER_GKE_LOCATION": "us-east1"})
    @mock.patch.dict("os.environ", {"GCP_TENANT_PROJECT": "test-project-234"})
    def test_pod_mutation_hook_composer_serverless(self):
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(namespace="test"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(
                            requests={"ephemeral-storage": "15G"}, limits={"ephemeral-storage": "20G"}
                        ),
                    )
                ]
            ),
        )

        pod_mutation_hook_composer_serverless(pod)

        assert pod.metadata.namespace == "composer-user-workloads"
        assert pod.spec.containers[0].resources is None

    @pytest.mark.parametrize(
        "resources, expected_machine_type",
        [
            (None, "e2-custom-small-2048"),
        ]
        + [
            # Tests for CPU.
            (
                k8s.V1ResourceRequirements(requests={"cpu": "1"}, limits={"cpu": "2"}),
                "e2-custom-2-8192",
            ),
            (
                k8s.V1ResourceRequirements(requests={"cpu": "1"}, limits={"cpu": "2"}),
                "e2-custom-2-8192",
            ),
            (
                k8s.V1ResourceRequirements(requests={}, limits={}),
                "e2-custom-small-2048",
            ),
            (
                k8s.V1ResourceRequirements(requests={"cpu": "3000m"}, limits={"cpu": "1000m"}),
                "e2-custom-4-16384",
            ),
            (
                k8s.V1ResourceRequirements(requests={"cpu": "2.5"}, limits={"cpu": "0.5"}),
                "e2-custom-4-16384",
            ),
            (
                k8s.V1ResourceRequirements(requests={"cpu": "1000m"}, limits={"cpu": "3000m"}),
                "e2-custom-4-16384",
            ),
            (
                k8s.V1ResourceRequirements(requests={"cpu": "0.5"}, limits={"cpu": "2.5"}),
                "e2-custom-4-16384",
            ),
            (
                k8s.V1ResourceRequirements(requests={"cpu": "6.5"}),
                "e2-custom-8-32768",
            ),
            (
                k8s.V1ResourceRequirements(limits={"cpu": "3.89"}),
                "e2-custom-4-16384",
            ),
            (
                k8s.V1ResourceRequirements(limits={"cpu": "0.1"}),
                "e2-custom-micro-2048",
            ),
            (
                k8s.V1ResourceRequirements(limits={"cpu": "0.26"}),
                "e2-custom-small-2048",
            ),
            (
                k8s.V1ResourceRequirements(limits={"cpu": "0.6"}),
                "e2-custom-medium-4096",
            ),
            (
                k8s.V1ResourceRequirements(limits={"cpu": "3.9"}),
                "e2-custom-4-16384",
            ),
            (
                k8s.V1ResourceRequirements(limits={"cpu": "4.0000000001"}),
                "e2-custom-4-16384",
            ),
            (
                k8s.V1ResourceRequirements(limits={"cpu": "300"}),
                "e2-custom-32-131072",
            ),
        ]
        + [
            (
                k8s.V1ResourceRequirements(limits={"cpu": str(x)}),
                f"e2-custom-{x}-{x * 4 * 1024}",
            )
            for x in range(2, 34, 2)
        ]
        # Tests for memory.
        + [
            (
                k8s.V1ResourceRequirements(requests={"wrong-key": "1G"}, limits={"wrong-key": "2G"}),
                "e2-custom-small-2048",
            ),
            (
                k8s.V1ResourceRequirements(requests={"memory": "1G"}, limits={"memory": "3G"}),
                "e2-custom-small-3072",
            ),
            (
                k8s.V1ResourceRequirements(requests={"memory": "3G"}, limits={"memory": "1G"}),
                "e2-custom-small-3072",
            ),
            (
                k8s.V1ResourceRequirements(requests={"memory": "1G"}),
                "e2-custom-small-2048",
            ),
            (
                k8s.V1ResourceRequirements(limits={"memory": "4G"}),
                "e2-custom-small-4096",
            ),
            (
                k8s.V1ResourceRequirements(limits={"memory": "2.5G"}),
                "e2-custom-small-3072",
            ),
            (
                k8s.V1ResourceRequirements(limits={"memory": "3.0000000001G"}),
                "e2-custom-small-3072",
            ),
            (
                k8s.V1ResourceRequirements(limits={"memory": "0.1G"}),
                "e2-custom-small-2048",
            ),
            (
                k8s.V1ResourceRequirements(limits={"cpu": "32", "memory": "128G"}),
                "e2-custom-32-131072",
            ),
            (
                k8s.V1ResourceRequirements(limits={"cpu": "32", "memory": "129G"}),
                "e2-custom-32-131072",
            ),
            (
                k8s.V1ResourceRequirements(limits={"cpu": "4", "memory": "0.1G"}),
                "e2-custom-4-2048",
            ),
            (
                k8s.V1ResourceRequirements(limits={"cpu": "4", "memory": "100G"}),
                "e2-custom-4-32768",
            ),
        ],
    )
    def test_get_composer_serverless_machine_type(self, resources, expected_machine_type):
        actual_machine_type = _get_composer_serverless_machine_type(resources)

        assert actual_machine_type == expected_machine_type

    @mock.patch("airflow.composer.kubernetes.utils.kubernetes_stream", autospec=True)
    def test_exec_on_placeholder_pod(self, kubernetes_stream_mock):
        self_mock = mock.Mock()
        pod_mock = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="pod-name", namespace="pod-namespace"))
        kubernetes_stream_mock.return_value = mock.Mock(
            is_open=mock.Mock(return_value=True),
            peek_stdout=mock.Mock(side_effect=[True, False]),
            read_stdout=mock.Mock(return_value="content"),
            peek_stderr=mock.Mock(return_value=False))

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

    @mock.patch("airflow.composer.kubernetes.utils.kubernetes_stream", autospec=True)
    def test_exec_on_placeholder_pod_error(self, kubernetes_stream_mock):
        self_mock = mock.Mock()
        pod_mock = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="pod-name", namespace="pod-namespace"))
        kubernetes_stream_mock.return_value = mock.Mock(
            is_open=mock.Mock(return_value=True),
            peek_stdout=mock.Mock(return_value=False),
            peek_stderr=mock.Mock(side_effect=[True, False]),
            read_stderr=mock.Mock(return_value="API error"))

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

    @mock.patch("airflow.composer.kubernetes.utils.exec_on_placeholder_pod", autospec=True)
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
        "container_statuses, expected_result", [
            ([{"container": "airflow-xcom-sidecar", "state": "RUNNING"},
              {"container": "base", "state": "TERMINATED"}], True),
            ([{"container": "airflow-xcom-sidecar", "state": "RUNNING"},
              {"container": "base", "state": "RUNNING"}], False),
        ]
    )
    @mock.patch("airflow.composer.kubernetes.utils.get_peer_vm_pod_container_statuses", autospec=True)
    def test_is_kubernetes_pod_operator_base_container_terminated(self,
            get_peer_vm_pod_container_statuses_mock, container_statuses, expected_result):
        get_peer_vm_pod_container_statuses_mock.return_value = container_statuses
        self_mock = mock.Mock()
        pod_mock = mock.Mock()

        actual_result = is_kubernetes_pod_operator_base_container_terminated(self_mock, pod_mock)

        get_peer_vm_pod_container_statuses_mock.assert_called_with(self_mock, pod=pod_mock)
        assert actual_result == expected_result

    @mock.patch("airflow.composer.kubernetes.utils.get_peer_vm_pod_container_statuses", autospec=True)
    def test_is_kubernetes_pod_operator_base_container_terminated_unexpected_list_of_containers(
            self, get_peer_vm_pod_container_statuses_mock):
        get_peer_vm_pod_container_statuses_mock.return_value = [
            {"container": "airflow-xcom-sidecar", "state": "RUNNING"}]

        self_mock = mock.Mock()
        pod_mock = mock.Mock()

        with pytest.raises(ValueError) as exc:
            is_kubernetes_pod_operator_base_container_terminated(self_mock, pod_mock)

        get_peer_vm_pod_container_statuses_mock.assert_called_with(self_mock, pod=pod_mock)
        assert str(exc.value) == (
            "Unexpected list of containers in KubernetesPodOperator pod, container statuses: "
            "[{'container': 'airflow-xcom-sidecar', 'state': 'RUNNING'}]")

    @pytest.mark.parametrize(
        "response, expected_result", [
            (b"ggEBghMBeyJrZXk6IjogInZhbHVlIn0KiAID6A==", '{"key:": "value"}\n'),
            (b"ggEBggUBMjIyCogCA+g=", "222\n")
        ]
    )
    def test_parse_payload_from_peer_vm_exec_response(self, response, expected_result):
        actual_result = parse_payload_from_peer_vm_exec_response(response)

        assert actual_result == expected_result

    def test_parse_payload_from_peer_vm_exec_response_big_payload_many_frames(self):
        peer_vm_exec_response_100000_length_list = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "../test_data/peer_vm_exec_response_100000_length_list")
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
