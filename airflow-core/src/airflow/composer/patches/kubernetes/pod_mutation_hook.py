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

import logging
import math
import os

import yaml
from kubernetes.client import Configuration, models as k8s
from kubernetes.utils import parse_quantity

from airflow.composer.patches.core.utils import get_composer_gke_cluster_host
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator

MAX_RESOURCES_DISK_SIZE_GB = 100
USER_WORKLOADS_NAMESPACE = "composer-user-workloads"
USE_WORKLOAD_IDENTITY_SERVICE_LABEL = "node.gke.io/use-workload-identity-service"
GCE_VM_ANNOTATION = "node.gke.io/gce-vm"
EPS = 1e-9

logger = logging.getLogger(__name__)


def mutate(pod: k8s.V1Pod):
    is_k8s_executor_pod = (
        pod.spec
        and pod.spec.containers
        and pod.spec.containers[0].env
        and any(env_var.name == "AIRFLOW_IS_K8S_EXECUTOR_POD" for env_var in pod.spec.containers[0].env)
    )

    # In case of running pod by KPO or KubernetesExecutor we should adjust pod spec.
    # Note, that we check below if cluster host where pod will be deployed is a Composer GKE cluster host, to
    # account for GKEStartPodOperator which runs pods in external GKE cluster.
    # In case of scheduler (KubernetesExecutor), the logic with checking host doesn't work, so we use
    # different approach with is_k8s_executor_pod.
    pod_running_in_composer_gke = (
        is_k8s_executor_pod or Configuration.get_default_copy().host == get_composer_gke_cluster_host()
    )
    if not pod_running_in_composer_gke:
        return

    logger.info("Modifying pod spec")
    pod.metadata = PodGenerator.reconcile_metadata(pod.metadata, _get_peer_vm_pod_metadata(pod))

    # Clear resources, they are provided via pod.metadata.
    pod.spec.containers[0].resources = None


def _get_peer_vm_pod_metadata(pod: k8s.V1Pod):
    """Return metadata for container-based tasks."""
    tenant_project_id = os.environ["GCP_TENANT_PROJECT"]

    resources_disk_size_gb = 0
    if pod.spec.containers[0].resources:
        requests_gb = 0
        limits_gb = 0
        if pod.spec.containers[0].resources.requests:
            requests_gb = math.ceil(
                parse_quantity(pod.spec.containers[0].resources.requests.get("ephemeral-storage", "0"))
                / 1000
                / 1000
                / 1000
            )  # round up to integer number of GBs.
        if pod.spec.containers[0].resources.limits:
            limits_gb = math.ceil(
                parse_quantity(pod.spec.containers[0].resources.limits.get("ephemeral-storage", "0"))
                / 1000
                / 1000
                / 1000
            )  # round up to integer number of GBs.
        resources_disk_size_gb = max(requests_gb, limits_gb)
    if resources_disk_size_gb > MAX_RESOURCES_DISK_SIZE_GB:
        logger.warning(
            "Resources disk size is %sGB which is greater than maximum allowed %sGB",
            resources_disk_size_gb,
            MAX_RESOURCES_DISK_SIZE_GB,
        )
        resources_disk_size_gb = MAX_RESOURCES_DISK_SIZE_GB
    disk_size_gb = 30 + resources_disk_size_gb  # add 30 GB for operating system, container images etc.

    return k8s.V1ObjectMeta(
        # All container-based tasks should be running in a dedicated "user workloads namespace".
        namespace=USER_WORKLOADS_NAMESPACE,
        labels={
            USE_WORKLOAD_IDENTITY_SERVICE_LABEL: "true",
        },
        annotations={
            GCE_VM_ANNOTATION: yaml.dump(
                {
                    "selector": {
                        "matchLabels": {
                            "machineType": _get_peer_vm_machine_type(pod.spec.containers[0].resources),
                            "diskSizeGb": str(disk_size_gb),
                        }
                    },
                    "logging": ["Workload", "System"],
                    "vmServiceAccount": f"peervm-vm@{tenant_project_id}.iam.gserviceaccount.com",
                }
            ),
        },
    )


def _get_peer_vm_machine_type(resources: k8s.V1ResourceRequirements) -> str:
    """Return machine type for container-based tasks."""
    machine_series = "e2"
    cpu_amount, cpu_string = _get_peer_vm_machine_cpu(resources)
    memory_string = _get_peer_vm_machine_memory(resources, cpu_amount)

    return f"{machine_series}-custom-{cpu_string}-{memory_string}"


def _get_peer_vm_machine_cpu(resources: k8s.V1ResourceRequirements) -> tuple[float, str]:
    requests_cpu = None
    limits_cpu = None
    if resources and resources.requests and resources.requests.get("cpu"):
        requests_cpu = resources.requests["cpu"]
        requests_cpu = float(requests_cpu[:-1]) / 1000 if requests_cpu.endswith("m") else float(requests_cpu)
    if resources and resources.limits and resources.limits.get("cpu"):
        limits_cpu = resources.limits["cpu"]
        limits_cpu = float(limits_cpu[:-1]) / 1000 if limits_cpu.endswith("m") else float(limits_cpu)
    if (requests_cpu is not None) and (limits_cpu is not None):
        desired_cpu_amount = max(requests_cpu, limits_cpu)
    else:
        desired_cpu_amount = requests_cpu or limits_cpu or 0.5

    # List of valid CPU amount and corresponding presentation.
    valid_cpu_values = [
        (0.25, "micro"),
        (0.5, "small"),
        (1, "medium"),
    ] + [(x, str(x)) for x in range(2, 34, 2)]
    # Find the lowest valid CPU amount that is greater or equal than desired.
    for valid_cpu_amount, valid_cpu_string in valid_cpu_values:
        if desired_cpu_amount < valid_cpu_amount + EPS:
            return valid_cpu_amount, valid_cpu_string

    logger.warning(
        "Resources CPU is %s which is greater than maximum allowed %s",
        desired_cpu_amount,
        valid_cpu_values[-1][1],
    )
    return valid_cpu_values[-1]


def _get_peer_vm_machine_memory(resources: k8s.V1ResourceRequirements, cpu: float) -> str:
    requests_memory_gb = None
    limits_memory_gb = None
    if resources and resources.requests and resources.requests.get("memory"):
        requests_memory_gb = parse_quantity(resources.requests["memory"]) / 1000 / 1000 / 1000
    if resources and resources.limits and resources.limits.get("memory"):
        limits_memory_gb = parse_quantity(resources.limits["memory"]) / 1000 / 1000 / 1000
    if (requests_memory_gb is not None) and (limits_memory_gb is not None):
        desired_memory_gb_amount = max(requests_memory_gb, limits_memory_gb)
    else:
        desired_memory_gb_amount = requests_memory_gb or limits_memory_gb or (cpu * 4)

    # List of valid memory amount with corresponding presentation (based on given CPU).
    # TODO: investigate why KPO/KE tasks are failing with Peer VM having 1GB memory.
    valid_memory_gb_values = [(x, str(1024 * x)) for x in range(2, 129) if 0.5 - EPS < x / cpu < 8 + EPS]
    for valid_memory_gb_amount, valid_memory_gb_string in valid_memory_gb_values:
        if desired_memory_gb_amount < valid_memory_gb_amount + EPS:
            return valid_memory_gb_string

    logger.warning(
        "Resources memory is %sGB which is greater than maximum allowed %sGB for the current amount of CPU "
        "which is %s",
        desired_memory_gb_amount,
        valid_memory_gb_values[-1][0],
        cpu,
    )
    return valid_memory_gb_values[-1][1]
