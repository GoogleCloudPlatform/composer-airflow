#
# Copyright 2020 Google LLC
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

import base64
import json
import logging
import math
import os
from contextlib import closing
from typing import TYPE_CHECKING

import yaml
from kubernetes.client import models as k8s
from kubernetes.client.exceptions import ApiException
from kubernetes.stream import stream as kubernetes_stream
from kubernetes.utils import parse_quantity
from websockets.frames import Frame
from websockets.streams import StreamReader

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults

if TYPE_CHECKING:
    from kubernetes.client.models.v1_pod import V1Pod
    from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager


PEER_VM_PLACEHOLDER_CONTAINER = "peervm-placeholder"
PEER_VM_ENDPOINT_ANNOTATION = "node.gke.io/peer-vm-endpoint"

USER_WORKLOADS_NAMESPACE = "composer-user-workloads"
USE_WORKLOAD_IDENTITY_SERVICE_LABEL = "node.gke.io/use-workload-identity-service"
GCE_VM_ANNOTATION = "node.gke.io/gce-vm"
MAX_RESOURCES_DISK_SIZE_GB = 100
EPS = 1e-9

log = logging.getLogger(__file__)


class PeerVmPlaceholderPodShutDownException(Exception):
    """Exception raised when Peer VM placeholder pod exec returns 137 exit code."""
    pass

class PeerVmPlaceholderPodContainerNotFoundException(Exception):
    """Exception raised when Peer VM placeholder pod container is not found."""
    pass


def pod_mutation_hook_composer_serverless(pod: k8s.V1Pod):
    pod.metadata = PodGenerator.reconcile_metadata(pod.metadata, _get_composer_serverless_pod_metadata(pod))

    # Clear resources, they are provided via pod.metadata.
    pod.spec.containers[0].resources = None


def _get_composer_serverless_pod_metadata(pod: k8s.V1Pod):
    """Returns metadata for container-based tasks in Composer serverless.

    Refer to go/composer25-kpo-k8s-executor for details.
    """
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
        log.warning(
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
                            "machineType": _get_composer_serverless_machine_type(
                                pod.spec.containers[0].resources
                            ),
                            "diskSizeGb": str(disk_size_gb),
                        }
                    },
                    "logging": ["Workload", "System"],
                    "vmServiceAccount": f"peervm-vm@{tenant_project_id}.iam.gserviceaccount.com",
                }
            ),
        },
    )


def _get_composer_serverless_machine_type(resources: k8s.V1ResourceRequirements) -> str:
    """Returns machine type for container-based tasks in Composer serverless.

    Refer to go/composer25-kpo-k8s-executor for details.
    """
    machine_series = "e2"
    cpu_amount, cpu_string = _get_composer_serverless_machine_cpu(resources)
    memory_string = _get_composer_serverless_machine_memory(resources, cpu_amount)

    return f"{machine_series}-custom-{cpu_string}-{memory_string}"


def _get_composer_serverless_machine_cpu(resources: k8s.V1ResourceRequirements) -> tuple[float, str]:
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

    log.warning(
        "Resources CPU is %s which is greater than maximum allowed %s",
        desired_cpu_amount,
        valid_cpu_values[-1][1],
    )
    return valid_cpu_values[-1]


def _get_composer_serverless_machine_memory(resources: k8s.V1ResourceRequirements, cpu: float) -> str:
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

    log.warning(
        "Resources memory is %sGB which is greater than maximum allowed %sGB for the current amount of CPU "
        "which is %s",
        desired_memory_gb_amount,
        valid_memory_gb_values[-1][0],
        cpu,
    )
    return valid_memory_gb_values[-1][1]


def exec_on_placeholder_pod(self: PodManager, pod: V1Pod, command: list[str]):
    """Runs exec command on Peer VM placeholder pod.

    Args:
        self: instance of PodManager.
        pod: k8s placeholder pod.
        command: command to run as a list of arguments.
    Returns:
        Response of exec command.
    Raises:
        PeerVmPlaceholderPodContainerNotFoundException: if Peer VM placeholder pod container is not found.
        PeerVmPlaceholderPodShutDownException: if Peer VM placeholder pod is shut down.
    """
    try:
        with closing(
            kubernetes_stream(
                self._client.connect_get_namespaced_pod_exec,
                pod.metadata.name,
                pod.metadata.namespace,
                container=PEER_VM_PLACEHOLDER_CONTAINER,
                command=command,
                stdin=True,
                stdout=True,
                stderr=True,
                tty=False,
                _preload_content=False,
            )
        ) as resp:
            result = ""
            while resp.is_open():
                resp.update(timeout=1)

                while resp.peek_stdout():
                    result += resp.read_stdout()

                error_res = ""
                while resp.peek_stderr():
                    error_res += resp.read_stderr()
                if error_res:
                    raise AirflowException(f"There was an error in calling kubernetes API: {error_res}")

                if result:
                    break

            try:
                return_code = resp.returncode
            except ValueError:
                self.log.debug("Unable to retrieve exit code, this happens when pod is shut down")
                raise PeerVmPlaceholderPodShutDownException("Error on parsing exit code")
            self.log.debug("Exec command response: %s, return code: %s", result, return_code)

            if return_code:
                if return_code == 137:
                    raise PeerVmPlaceholderPodShutDownException("Got 137 exit code on exec")
                else:
                    raise AirflowException(
                        f"There was an error in calling kubernetes API, return code: {return_code}")

            return result
    except ApiException as exc:
        if "container not found" in exc.reason:
            raise PeerVmPlaceholderPodContainerNotFoundException(exc.reason)
        raise


def get_peer_vm_pod_container_statuses(self: PodManager, pod: V1Pod):
    """Returns statuses of the containers of Peer VM pod.

    Args:
        self: instance of PodManager.
        pod: k8s placeholder pod.
    Returns:
        List of dictionaries with container statuses, e.g.
        [{"container":"airflow-xcom-sidecar","state":"RUNNING"},{"container":"base","state":"TERMINATED"}].
    """
    resp = exec_on_placeholder_pod(self, pod=pod, command=["placeholder-pod", "container-statuses"])
    return json.loads(resp)


def is_kubernetes_pod_operator_base_container_terminated(self: PodManager, pod: V1Pod):
    """Returns whether base container of KubernetesPodOperator pod is terminated.

    KubernetesPodOperator pod can have 1 or 2 containers:
    - in case do_xcom_push=False - one base container
    - in case do_xcom_push=True - 2 containers: base container and xcom sidecar container

    Args:
        self: instance of PodManager.
        pod: k8s placeholder pod.
    """
    container_statuses = get_peer_vm_pod_container_statuses(self, pod=pod)

    # Note: "base" is just a default name for base container of KubernetesPodOperator, user can override
    # its name in operator parameters.
    base_container_status = [
        status
        for status in container_statuses
        if status["container"] != PodDefaults.SIDECAR_CONTAINER_NAME
    ]
    if len(base_container_status) != 1:
        raise ValueError(
            "Unexpected list of containers in KubernetesPodOperator pod, container statuses: "
            f"{container_statuses}")

    return base_container_status[0]["state"] == "TERMINATED"


def parse_payload_from_peer_vm_exec_response(response):
    """Parses payload from response of exec command ran on Peer VM pod.

    Args:
        response: base64 encoded websocket frames stream.
    Returns:
        Payload.
    """
    def _get_frame(read_exact):
        frame = yield from Frame.parse(read_exact=read_exact, mask=False)
        yield frame

    reader = StreamReader()
    reader.feed_data(base64.b64decode(response))

    frames = []
    while True:
        try:
            frame = next(_get_frame(reader.read_exact))
        except Exception as e:
            raise AirflowException(f"Unable to parse response from exec command: {e}")
        if frame is None:
            break

        frames.append(frame)

    # Collect payload from frames:
    # - skip header and footer frames
    # - decode content from bytes to string
    payload = "".join([
        frame.data[1:].decode("utf-8")
        for frame in frames[1:-1]
    ])

    return payload
