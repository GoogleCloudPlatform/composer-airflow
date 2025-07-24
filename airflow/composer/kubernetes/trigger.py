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

from airflow.composer.kubernetes.utils import (
    PEER_VM_PLACEHOLDER_CONTAINER,
    await_pod_endpoint_creation,
    get_peer_vm_pod_container_statuses,
)
from airflow.providers.cncf.kubernetes.triggers.pod import ContainerState

if TYPE_CHECKING:
    from kubernetes.client.models.v1_pod import V1Pod


log = logging.getLogger(__name__)


def patch_kubernetes_hook():
    from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook

    if not getattr(KubernetesHook.__init__, "_composer_patched", False):
        log.info("Patching kubernetes hook init")
        KubernetesHook.__init__ = _composer_kubernetes_hook_init(KubernetesHook.__init__)

    if not getattr(KubernetesHook.get_conn, "_composer_patched", False):
        log.info("Patching kubernetes hook get_conn")
        KubernetesHook.get_conn = _composer_kubernetes_hook_get_conn(KubernetesHook.get_conn)
        setattr(KubernetesHook.get_conn, "_composer_patched", True)


def patch_define_container_state():
    from airflow.providers.cncf.kubernetes.triggers.pod import KubernetesPodTrigger

    if not getattr(KubernetesPodTrigger.define_container_state, "_composer_patched", False):
        return

    log.info("Patching define_container_state start")

    KubernetesPodTrigger.define_container_state = _composer_define_container_state(
        KubernetesPodTrigger.define_container_state
    )

    log.info("Patching define_container_state finish")
    setattr(KubernetesPodTrigger.define_container_state, "_composer_patched", True)


def _composer_kubernetes_hook_init(f):
    @functools.wraps(f)
    def wrapper(self, config_dict: dict | None = None, *args, **kwargs):
        return_value = f(self, *args, **kwargs)

        if not hasattr(self, "config_dict"):
            self.config_dict = config_dict
        return return_value

    return wrapper


def _composer_define_container_state(f):
    @functools.wraps(f)
    def wrapper(self, pod: V1Pod) -> ContainerState:
        from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
        from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager

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

        await_pod_endpoint_creation(self, pod, remote_pod)

        # If user's container had finished execution earlier than peer_vm_endpoint has been created,
        # then this function can't create a Handshake with PeerVM container and fails with error.
        pod_containers = get_peer_vm_pod_container_statuses(pod_manager, pod=pod)

        if pod_containers is None:
            return ContainerState.UNDEFINED

        container = next(c for c in pod_containers if c["container"] == self.base_container_name)
        return container["state"].lower()

    return wrapper


def _composer_kubernetes_hook_get_conn(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        from kubernetes import client, config

        from airflow.providers.cncf.kubernetes.hooks.kubernetes import (
            LOADING_KUBE_CONFIG_FILE_RESOURCE,
            _get_bool,
        )
        from airflow.providers.cncf.kubernetes.kube_client import _disable_verify_ssl, _enable_tcp_keepalive

        # use original get_conn
        if not self.config_dict:
            return f(self, *args, **kwargs)

        cluster_context = self._coalesce_param(self.cluster_context, self._get_field("cluster_context"))
        disable_verify_ssl = self._coalesce_param(
            self.disable_verify_ssl, _get_bool(self._get_field("disable_verify_ssl"))
        )
        disable_tcp_keepalive = self._coalesce_param(
            self.disable_tcp_keepalive, _get_bool(self._get_field("disable_tcp_keepalive"))
        )

        if disable_verify_ssl is True:
            _disable_verify_ssl()
        if disable_tcp_keepalive is not True:
            _enable_tcp_keepalive()

        self.log.info(LOADING_KUBE_CONFIG_FILE_RESOURCE.format("config dictionary"))
        self._is_in_cluster = False
        config.load_kube_config_from_dict(
            config_dict=self.config_dict,
            client_configuration=self.client_configuration,
            context=cluster_context,
        )
        return client.ApiClient()

    return wrapper
