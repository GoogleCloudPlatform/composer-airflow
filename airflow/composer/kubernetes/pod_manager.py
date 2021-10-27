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
import os
import time

from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
from google.cloud.logging_v2.types import ListLogEntriesRequest

from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager, PodPhase

PEER_VM_PLACEHOLDER_CONTAINER = "peervm-placeholder"
PEER_VM_NAME_ANNOTATION = "node.gke.io/peer-vm-name"
# This is the time to sleep in seconds before first and every other attempt to read log entries
# from Cloud Logging. Note that this also defines time between placeholder container not running
# and last attempt to read logs, so it should account for propagation delay of logs from VM to
# Cloud Logging.
SLEEP_BETWEEN_PEER_VM_LOGS_STREAMING_ITERATIONS = 15


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
        remote_pod = self.read_pod(pod)

        if remote_pod.spec.containers[0].name != PEER_VM_PLACEHOLDER_CONTAINER:
            # KPO pod is running as regular k8s pod, execute native implementation of the
            # fetch_container_logs method.
            return f(self, *args, **kwargs)

        self.log.info("Fetching logs from Cloud Logging")
        # Placeholder pod can get to the 'Running' state but annotation with Peer VM name may be absent,
        # this can happen (as observed) if VM is still being created.
        while remote_pod.status.phase == PodPhase.RUNNING and not remote_pod.metadata.annotations.get(
            PEER_VM_NAME_ANNOTATION
        ):
            self.log.info("Pod is still in the 'Running' phase")
            time.sleep(5)
            remote_pod = self.read_pod(pod)

        peer_vm_name = remote_pod.metadata.annotations.get(PEER_VM_NAME_ANNOTATION)
        # If annotation with Peer VM name is missing and we are here, that means that placeholder pod changed
        # its state to some other than 'Running' (most likely some terminal state) without VM being finally
        # successfully created.
        if peer_vm_name is None:
            self.log.info("Not found %s annotation for pod", PEER_VM_NAME_ANNOTATION)
            return

        client = LoggingServiceV2Client()
        _stream_peer_vm_logs(
            self,
            pod=pod,
            client=client,
            project_id=os.environ.get("GCP_TENANT_PROJECT"),
            peer_vm_name=peer_vm_name,
            since_timestamp=remote_pod.metadata.creation_timestamp.strftime("%Y-%m-%dT%H:%M:%S") + "Z",
            seen_insert_ids=set(),
        )

    return wrapper


def _stream_peer_vm_logs(self, pod, client, project_id, peer_vm_name, since_timestamp, seen_insert_ids):
    """Streams Peer VM logs of given k8s placeholder pod to self.log logger.

    Args:
         pod: k8s placeholder pod.
         client: client to query Cloud Logging logs.
         project_id: id of the project where Peer VM is located.
         peer_vm_name: name of the Peer VM.
         since_timestamp: timestamp since query logs in RFC 3339 format.
         seen_insert_ids: set that contains insert_ids of already seen logs.
    """
    is_last_iteration = not self.container_is_running(pod, container_name=PEER_VM_PLACEHOLDER_CONTAINER)
    time.sleep(SLEEP_BETWEEN_PEER_VM_LOGS_STREAMING_ITERATIONS)

    # We want to read k8s_container logs for given project and Peer VM name (VM name is unique
    # across regions in project) starting with given timestamp.
    log_filter = "\n".join(
        [
            'resource.type="k8s_container"',
            f'resource.labels.project_id="{project_id}"',
            f'labels.peervm_name="{peer_vm_name}"',
            f'timestamp>="{since_timestamp}"',
        ]
    )
    request = ListLogEntriesRequest(
        resource_names=[f"projects/{project_id}"],
        filter=log_filter,
        order_by="timestamp asc",
        page_size=1000,
    )
    self.log.debug("Reading log entries using filter: %s", log_filter)
    response = client.list_log_entries(request=request)

    for entry in response:
        if entry.insert_id in seen_insert_ids:
            continue
        self.log.info(entry.text_payload)
        seen_insert_ids.add(entry.insert_id)

    if is_last_iteration:
        return

    _stream_peer_vm_logs(
        self,
        pod=pod,
        client=client,
        project_id=project_id,
        peer_vm_name=peer_vm_name,
        since_timestamp=since_timestamp,
        seen_insert_ids=seen_insert_ids,
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
