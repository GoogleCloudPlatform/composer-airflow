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

import base64
import json
from contextlib import closing
from typing import TYPE_CHECKING

from kubernetes.client.exceptions import ApiException
from kubernetes.stream import stream as kubernetes_stream
from websockets.frames import Frame
from websockets.streams import StreamReader

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults

if TYPE_CHECKING:
    from kubernetes.client.models.v1_pod import V1Pod

    from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager


PEER_VM_PLACEHOLDER_CONTAINER = "peervm-placeholder"
PEER_VM_ENDPOINT_ANNOTATION = "node.gke.io/peer-vm-endpoint"


class PeerVmPlaceholderPodShutDownException(Exception):
    """Exception raised when Peer VM placeholder pod exec returns 137 exit code."""

    pass


class PeerVmPlaceholderPodContainerNotFoundException(Exception):
    """Exception raised when Peer VM placeholder pod container is not found."""

    pass


def exec_on_placeholder_pod(self: PodManager, pod: V1Pod, command: list[str]):
    """
    Run exec command on Peer VM placeholder pod.

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
                raise AirflowException(
                    f"There was an error in calling kubernetes API, return code: {return_code}"
                )

            return result
    except ApiException as exc:
        if "container not found" in exc.reason:
            raise PeerVmPlaceholderPodContainerNotFoundException(exc.reason)
        raise


def get_peer_vm_pod_container_statuses(self: PodManager, pod: V1Pod):
    """
    Return statuses of the containers of Peer VM pod.

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
    """
    Return whether base container of KubernetesPodOperator pod is terminated.

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
        status for status in container_statuses if status["container"] != PodDefaults.SIDECAR_CONTAINER_NAME
    ]
    if len(base_container_status) != 1:
        raise ValueError(
            "Unexpected list of containers in KubernetesPodOperator pod, container statuses: "
            f"{container_statuses}"
        )

    return base_container_status[0]["state"] == "TERMINATED"


def parse_payload_from_peer_vm_exec_response(response):
    """
    Parse payload from response of exec command ran on Peer VM pod.

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
    payload = "".join([frame.data[1:].decode("utf-8") for frame in frames[1:-1]])

    return payload
