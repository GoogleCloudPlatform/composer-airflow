from __future__ import annotations

import unittest
from unittest import mock

import yaml
from kubernetes.client import models as k8s
from parameterized import parameterized

from airflow.composer.kubernetes.utils import (
    _get_composer_serverless_machine_type,
    _get_composer_serverless_pod_metadata,
    pod_mutation_hook_composer_serverless,
)


class TestUtils(unittest.TestCase):
    @mock.patch.dict("os.environ", {"COMPOSER_GKE_LOCATION": "us-east1"})
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
                                "region": "us-east1",
                                "diskSizeGb": "20",
                            },
                        },
                        "logging": ["Workload", "System"],
                        "vmServiceAccount": "peervm-vm@test-project-234.iam.gserviceaccount.com",
                    }
                ),
            },
        )

    @parameterized.expand(
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
                "40",
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
                "37",
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
                "20",
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
                "39",
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
                "39",
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
                "31",
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
                "32",
            ),
            (
                k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")])),
                "20",
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
                "120",
            ),
        ]
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

    @parameterized.expand(
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
                "e2-custom-micro-1024",
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
                "e2-custom-small-1024",
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
                "e2-custom-small-1024",
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
        ]
    )
    def test_get_composer_serverless_machine_type(self, resources, expected_machine_type):
        actual_machine_type = _get_composer_serverless_machine_type(resources)

        assert actual_machine_type == expected_machine_type
