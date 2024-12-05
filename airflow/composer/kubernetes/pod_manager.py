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
"""Module that extends airflow.providers.cncf.kubernetes.utils.pod_manager module.

See go/composer25-kpo-logs-airflow-worker for implementation details.
"""
from __future__ import annotations

import functools
import json
import time
from typing import TYPE_CHECKING

import requests
import tenacity

from airflow.composer.kubernetes.utils import (
    PEER_VM_PLACEHOLDER_CONTAINER,
    PEER_VM_ENDPOINT_ANNOTATION,
    PeerVmPlaceholderPodContainerNotFoundException,
    PeerVmPlaceholderPodShutDownException,
    get_peer_vm_pod_container_statuses,
    is_kubernetes_pod_operator_base_container_terminated,
    exec_on_placeholder_pod,
    parse_payload_from_peer_vm_exec_response,
)
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager, PodPhase, EMPTY_XCOM_RESULT
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults

if TYPE_CHECKING:
    from kubernetes.client.models.v1_pod import V1Pod


# This is the time to sleep in seconds before first attempt to read log entries from Peer VM. Generally this
# is needed to allow Peer VM to finish required setup and be ready to for "container-statuses" command
# execution.
SLEEP_BEFORE_PEER_VM_LOGS_STREAMING = 10

# This is the time to sleep in seconds before first and every other attempt to read log entries
# from Peer VM. Note, that this also defines time between placeholder container not running
# and last attempt to read logs.
SLEEP_BETWEEN_PEER_VM_LOGS_STREAMING_ITERATIONS = 1


def patch_fetch_container_logs():
    if getattr(PodManager.fetch_container_logs, "_composer_patched", False):
        return

    PodManager.fetch_container_logs = _composer_fetch_container_logs(PodManager.fetch_container_logs)
    setattr(PodManager.fetch_container_logs, "_composer_patched", True)

    # Patch get_container_names method as well, needed for PodManager to start fetching container logs.
    PodManager.get_container_names = _composer_get_container_names(PodManager.get_container_names)

    # Patch container_is_running method, needed for PodManager to be able to check statuses of Peer VM
    # containers.
    PodManager.container_is_running = _composer_container_is_running(PodManager.container_is_running)

    # Patch extract_xcom_json method, needed for PodManager to be able to extract XCOM from Peer VM pod.
    PodManager.extract_xcom_json = _composer_extract_xcom_json(PodManager.extract_xcom_json)

    # Patch extract_xcom_kill method, needed for PodManager to be able to kill XCOM sidecar container on Peer
    # VM pod.
    PodManager.extract_xcom_kill = _composer_extract_xcom_kill(PodManager.extract_xcom_kill)


def _composer_fetch_container_logs(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        pod = kwargs["pod"]
        container_name = kwargs["container_name"]
        remote_pod = self.read_pod(pod)

        if remote_pod.spec.containers[0].name != PEER_VM_PLACEHOLDER_CONTAINER:
            # KPO pod is running as regular k8s pod, execute native implementation of the
            # fetch_container_logs method.
            return f(self, *args, **kwargs)

        self.log.info("Fetching container logs")
        # Placeholder pod can get to the 'Running' state but annotation with Peer VM endpoint may be absent,
        # this can happen (as observed) if VM is still being created.
        while remote_pod.status.phase == PodPhase.RUNNING and not remote_pod.metadata.annotations.get(
            PEER_VM_ENDPOINT_ANNOTATION
        ):
            self.log.info("Awaiting for pod to start execution")
            time.sleep(5)
            remote_pod = self.read_pod(pod)

        peer_vm_endpoint = remote_pod.metadata.annotations.get(PEER_VM_ENDPOINT_ANNOTATION)
        # If annotation with Peer VM endpoint is missing and we are here, that means that placeholder pod
        # changed its state to some other than 'Running' (most likely some terminal state) without VM being
        # finally successfully created.
        if peer_vm_endpoint is None:
            self.log.info("Not found %s annotation for pod", PEER_VM_ENDPOINT_ANNOTATION)
            return

        _stream_peer_vm_logs(
            self,
            pod=pod,
            container_name=container_name,
            peer_vm_endpoint=peer_vm_endpoint,
            after_timestamp=remote_pod.metadata.creation_timestamp.strftime("%Y-%m-%dT%H:%M:%S.0") + "Z",
        )

    return wrapper


def _stream_peer_vm_logs(self, pod, container_name, peer_vm_endpoint, after_timestamp):
    """Streams Peer VM logs of given k8s placeholder pod to self.log logger.

    Args:
         pod: k8s placeholder pod.
         container_name: name of the container to read logs.
         peer_vm_endpoint: endpoint of the Peer VM, to retrieve logs.
         after_timestamp: timestamp since query logs in RFC 3339 format.
    """
    time.sleep(SLEEP_BEFORE_PEER_VM_LOGS_STREAMING)

    while True:
        try:
            is_last_iteration = is_kubernetes_pod_operator_base_container_terminated(self, pod=pod)
        except PeerVmPlaceholderPodContainerNotFoundException:
            self.log.debug(
                "KubernetesPodOperator pod container is not found. Looks like it was terminated already.")
            is_last_iteration = True
        except PeerVmPlaceholderPodShutDownException:
            self.log.debug("KubernetesPodOperator pod is shut down.")
            is_last_iteration = True

        time.sleep(SLEEP_BETWEEN_PEER_VM_LOGS_STREAMING_ITERATIONS)

        url = f"http://{peer_vm_endpoint}:9080/logs"
        params = {
            "container_name": container_name,
            "after_timestamp": after_timestamp,
            "max_log_lines": 1000,
        }
        self.log.debug("Reading logs, url: %s, params: %s", url, params)
        try:
            response = requests.get(url, params=params)
        except Exception as e:
            self.log.debug("Exception occurred on request: %s", e)
        else:
            if response.status_code != 200:
                self.log.debug("Got %s response, reason: %s", response.status_code, response.reason)
            else:
                for log in response.json()["logs"] or []:
                    # Example of log: "2023-05-02T10:11:12.2Z stdout F Creating dataset"
                    after_timestamp, _, _, msg = log.split(" ", 3)
                    self.log.info(msg)

        if is_last_iteration:
            break


def _composer_get_container_names(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        container_names = f(self, *args, **kwargs)
        if PEER_VM_PLACEHOLDER_CONTAINER in container_names:
            # Hack. Pretend as it is a regular k8s pod (of KPO) with base container, so that PodManager will
            # start to fetch container logs.
            container_names = ["base"]

        return container_names

    return wrapper


def _composer_container_is_running(f):
    @functools.wraps(f)
    def wrapper(self, pod: V1Pod, container_name: str) -> bool:
        remote_pod = self.read_pod(pod)
        if remote_pod.spec.containers[0].name != PEER_VM_PLACEHOLDER_CONTAINER:
            # KPO pod is running as regular k8s pod, execute native implementation.
            return f(self, pod, container_name)

        container_statuses = get_peer_vm_pod_container_statuses(self, pod=pod)
        for container_status in container_statuses:
            if container_status["container"] == container_name:
                return container_status["state"] == "RUNNING"

        raise ValueError(f"Not found container named as '{container_name}' for pod '{pod.metadata.name}'")

    return wrapper


def _composer_extract_xcom_json(f):
    @functools.wraps(f)
    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def wrapper(self, pod: V1Pod) -> str:
        remote_pod = self.read_pod(pod)
        if remote_pod.spec.containers[0].name != PEER_VM_PLACEHOLDER_CONTAINER:
            # KPO pod is running as regular k8s pod, execute native implementation.
            return f(self, pod)

        command_to_extract_xcom = (
            f"if [ -s {PodDefaults.XCOM_MOUNT_PATH}/return.json ]; "
            f"then cat {PodDefaults.XCOM_MOUNT_PATH}/return.json; "
            f"else echo {EMPTY_XCOM_RESULT}; fi"
        )
        resp = exec_on_placeholder_pod(self, pod=pod, command=[
            "placeholder-pod", "exec", PodDefaults.SIDECAR_CONTAINER_NAME,
            json.dumps(["/bin/sh", "-c", command_to_extract_xcom]),
        ])

        if resp is not None:
            payload = parse_payload_from_peer_vm_exec_response(resp)

            if payload.rstrip() != EMPTY_XCOM_RESULT:
                # Note: result string is parsed to check if its valid json.
                # This function still returns a string which is converted into json in the calling method.
                json.loads(payload)

            return payload

        raise AirflowException(f"Failed to extract xcom from pod: {pod.metadata.name}")

    return wrapper


def _composer_extract_xcom_kill(f):
    @functools.wraps(f)
    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def wrapper(self, pod: V1Pod):
        remote_pod = self.read_pod(pod)
        if remote_pod.spec.containers[0].name != PEER_VM_PLACEHOLDER_CONTAINER:
            # KPO pod is running as regular k8s pod, execute native implementation.
            return f(self, pod)

        exec_on_placeholder_pod(self, pod=pod, command=[
            "placeholder-pod", "exec", PodDefaults.SIDECAR_CONTAINER_NAME,
            json.dumps(["/bin/sh", "-c", "kill -2 1"]),
        ])

    return wrapper
