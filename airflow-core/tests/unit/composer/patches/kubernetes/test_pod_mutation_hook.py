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
import yaml
from kubernetes.client import Configuration, models as k8s

from airflow.composer.patches.kubernetes.pod_mutation_hook import (
    _get_peer_vm_machine_type,
    _get_peer_vm_pod_metadata,
    mutate,
)


class TestPodMutationHook:
    @mock.patch(
        "airflow.composer.patches.kubernetes.pod_mutation_hook.get_composer_gke_cluster_host",
        mock.Mock(return_value="http://internal-cluster"),
    )
    @mock.patch.dict("os.environ", {"GCP_TENANT_PROJECT": "test-project-234"})
    def test_mutate_internal_gke_cluster(self):
        Configuration.set_default(Configuration(host="http://internal-cluster"))
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(namespace="test"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(limits={"cpu": "1"}),
                    )
                ]
            ),
        )

        mutate(pod)

        assert pod.metadata.namespace == "composer-user-workloads"
        assert pod.spec.containers[0].resources is None

    @mock.patch(
        "airflow.composer.patches.kubernetes.pod_mutation_hook.get_composer_gke_cluster_host",
        mock.Mock(return_value="http://internal-cluster"),
    )
    def test_mutate_external_gke_cluster(self):
        Configuration.set_default(Configuration(host="http://external-cluster"))
        pod = k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="test"))

        mutate(pod)

        assert pod.metadata.namespace == "test"

    @mock.patch(
        "airflow.composer.patches.kubernetes.pod_mutation_hook.get_composer_gke_cluster_host",
        mock.Mock(return_value="http://internal-cluster"),
    )
    @mock.patch.dict("os.environ", {"GCP_TENANT_PROJECT": "test-project-234"})
    def test_mutate_k8s_executor(self):
        Configuration.set_default(Configuration(host="http://external-cluster"))
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(namespace="test"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(limits={"cpu": "1"}),
                        env=[k8s.V1EnvVar(name="AIRFLOW_IS_K8S_EXECUTOR_POD", value="True")],
                    )
                ]
            ),
        )

        mutate(pod)

        assert pod.metadata.namespace == "composer-user-workloads"
        assert pod.spec.containers[0].resources is None

    @mock.patch.dict("os.environ", {"GCP_TENANT_PROJECT": "test-project-234"})
    def test_get_peer_vm_pod_metadata(self):
        actual = _get_peer_vm_pod_metadata(
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
    @mock.patch.dict("os.environ", {"GCP_TENANT_PROJECT": "test-project-234"})
    def test_get_peer_vm_pod_metadata_disk_size_gb(self, pod, expected_disk_size_gb):
        actual_pod_metadata = _get_peer_vm_pod_metadata(pod)

        actual_disk_size_gb = yaml.safe_load(actual_pod_metadata.annotations.get("node.gke.io/gce-vm"))[
            "selector"
        ]["matchLabels"]["diskSizeGb"]
        assert actual_disk_size_gb == expected_disk_size_gb

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
    def test_get_peer_vm_machine_type(self, resources, expected_machine_type):
        actual_machine_type = _get_peer_vm_machine_type(resources)

        assert actual_machine_type == expected_machine_type
