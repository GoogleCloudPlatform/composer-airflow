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

import functools
import logging
from typing import TYPE_CHECKING

from airflow.composer.patches.kubernetes.utils import (
    PEER_VM_ENDPOINT_ANNOTATION,
    PEER_VM_PLACEHOLDER_CONTAINER,
    PeerVmPlaceholderPodContainerNotFoundException,
    PeerVmPlaceholderPodShutDownException,
    await_pod_endpoint_creation,
    get_peer_vm_pod_container_statuses,
    write_logs_from_peer_vm,
)
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.triggers.pod import (
    ContainerState,
    KubernetesPodTrigger,
)
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager, PodPhase
from airflow.providers.google.cloud.triggers.kubernetes_engine import GKEStartPodTrigger

if TYPE_CHECKING:
    from kubernetes.client.models.v1_pod import V1Pod
    from pendulum import DateTime


log = logging.getLogger(__name__)


def patch():
    KubernetesPodTrigger.define_container_state = _composer_kubernetes_pod_trigger_define_container_state(
        KubernetesPodTrigger.define_container_state
    )
    KubernetesPodOperator._write_logs = _composer_kubernetes_pod_operator_write_logs(
        KubernetesPodOperator._write_logs
    )


def _composer_kubernetes_pod_trigger_define_container_state(f):
    @functools.wraps(f)
    def wrapper(self, pod: V1Pod) -> ContainerState:
        if isinstance(self, GKEStartPodTrigger):
            return f(self, pod)

        sync_hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_dict=self.config_dict,
            cluster_context=self.cluster_context,
        )
        pod_manager = PodManager(kube_client=sync_hook.core_v1_client)

        remote_pod = pod_manager.read_pod(pod)
        if remote_pod.spec.containers[0].name != PEER_VM_PLACEHOLDER_CONTAINER:
            # KPO pod is running as regular k8s pod, execute native implementation.
            return f(self, pod)

        if remote_pod.status.phase == PodPhase.PENDING:
            return ContainerState.UNDEFINED

        await_pod_endpoint_creation(pod_manager, pod, remote_pod)

        # If user's container had finished execution earlier than peer_vm_endpoint has been created,
        # then this function can't create a Handshake with PeerVM container and fails with error.
        try:
            pod_containers = get_peer_vm_pod_container_statuses(pod_manager, pod=pod)
        except PeerVmPlaceholderPodContainerNotFoundException:
            self.log.debug(
                "KubernetesPodOperator pod container is not found. Looks like it was terminated already."
            )
            return ContainerState.TERMINATED
        except PeerVmPlaceholderPodShutDownException:
            self.log.debug("KubernetesPodOperator pod is shut down.")
            return ContainerState.TERMINATED

        if pod_containers is None:
            return ContainerState.UNDEFINED

        container = next(c for c in pod_containers if c["container"] == self.base_container_name)
        return container["state"].lower()

    return wrapper


def _composer_kubernetes_pod_operator_write_logs(f):
    @functools.wraps(f)
    def wrapper(self, pod: V1Pod, follow: bool = False, since_time: DateTime | None = None) -> None:
        # self is an instance of KubernetesPodOperator

        remote_pod = self.pod_manager.read_pod(pod)
        if remote_pod.spec.containers[0].name != PEER_VM_PLACEHOLDER_CONTAINER:
            # KPO pod is running as regular k8s pod, execute native implementation.
            return f(self, pod, follow, since_time)

        write_logs_from_peer_vm(
            self.pod_manager,
            container_name=self.base_container_name,
            peer_vm_endpoint=remote_pod.metadata.annotations.get(PEER_VM_ENDPOINT_ANNOTATION),
            after_timestamp=remote_pod.metadata.creation_timestamp.strftime("%Y-%m-%dT%H:%M:%S.0") + "Z",
        )

    return wrapper
