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
import time

import requests

from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager, PodPhase

PEER_VM_PLACEHOLDER_CONTAINER = "peervm-placeholder"
PEER_VM_ENDPOINT_ANNOTATION = "node.gke.io/peer-vm-endpoint"
# This is the time to sleep in seconds before first and every other attempt to read log entries
# from Peer VM. Note that this also defines time between placeholder container not running
# and last attempt to read logs.
SLEEP_BETWEEN_PEER_VM_LOGS_STREAMING_ITERATIONS = 1


def patch_fetch_container_logs():
    if getattr(PodManager.fetch_container_logs, "_composer_patched", False):
        return

    PodManager.fetch_container_logs = _composer_fetch_container_logs(PodManager.fetch_container_logs)
    setattr(PodManager.fetch_container_logs, "_composer_patched", True)

    # Patch get_container_names method as well, needed for PodManager to start fetching container logs.
    PodManager.get_container_names = _composer_get_container_names(PodManager.get_container_names)


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
            self.log.info("Pod is still in the 'Running' phase")
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
    is_last_iteration = not self.container_is_running(pod, container_name=PEER_VM_PLACEHOLDER_CONTAINER)
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
        return

    _stream_peer_vm_logs(
        self,
        pod=pod,
        container_name=container_name,
        peer_vm_endpoint=peer_vm_endpoint,
        after_timestamp=after_timestamp,
    )


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
