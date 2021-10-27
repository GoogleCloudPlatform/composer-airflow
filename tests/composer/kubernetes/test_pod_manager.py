#
# Copyright 2021 Google LLC
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

import datetime
import unittest
from unittest import mock

from google.cloud.logging_v2.types import ListLogEntriesRequest, LogEntry
from google.protobuf.timestamp_pb2 import Timestamp

from airflow.composer.kubernetes.pod_manager import (
    _composer_fetch_container_logs,
    _stream_peer_vm_logs,
    patch_fetch_container_logs,
)
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager


class TestPodManager(unittest.TestCase):
    @mock.patch("airflow.composer.kubernetes.pod_manager._composer_fetch_container_logs", autospec=True)
    def test_patch_fetch_container_logs(self, composer_fetch_container_logs_mock):
        # Call twice to check patching occurres only once.
        patch_fetch_container_logs()
        patch_fetch_container_logs()

        composer_fetch_container_logs_mock.assert_called_once()
        assert getattr(PodManager.fetch_container_logs, "_composer_patched") is True

    def test_composer_fetch_container_logs_not_peer_vm(self):
        pod_mock = mock.Mock()
        container_mock = mock.Mock()
        container_mock.name = "base"
        remote_pod_mock = mock.Mock()
        remote_pod_mock.spec = mock.Mock(containers=[container_mock])

        def read_pod_side_effect(pod):
            assert pod == pod_mock
            return remote_pod_mock

        self_mock = mock.Mock(read_pod=mock.Mock(side_effect=read_pod_side_effect))

        result_mock = mock.Mock()

        def f_mock_side_effect(self, pod=None):
            assert self == self_mock
            assert pod == pod_mock
            return result_mock

        f_mock = mock.Mock(side_effect=f_mock_side_effect)

        result = _composer_fetch_container_logs(f_mock)(self_mock, pod=pod_mock)

        assert result == result_mock

    @mock.patch("airflow.composer.kubernetes.pod_manager.LoggingServiceV2Client", autospec=True)
    @mock.patch("airflow.composer.kubernetes.pod_manager._stream_peer_vm_logs", autospec=True)
    @mock.patch.dict("os.environ", {"GCP_PROJECT": "test-project"})
    def test_composer_fetch_container_logs_peer_vm(
        self, stream_peer_vm_logs_mock, logging_service_v2_client_mock
    ):
        pod_mock = mock.Mock()
        container_mock = mock.Mock()
        container_mock.name = "peervm-placeholder"
        remote_pod_mock_1 = mock.Mock()
        remote_pod_mock_1.spec = mock.Mock(containers=[container_mock])
        remote_pod_mock_1.status = mock.Mock(phase="Running")
        remote_pod_mock_1.metadata = mock.Mock(annotations={})
        remote_pod_mock_2 = mock.Mock()
        remote_pod_mock_2.spec = mock.Mock(containers=[container_mock])
        remote_pod_mock_2.status = mock.Mock(phase="Running")
        remote_pod_mock_2.metadata = mock.Mock(
            creation_timestamp=datetime.datetime(2023, 5, 2, 10, 11, 12),
            annotations={"node.gke.io/peer-vm-name": "standard-micro-123"},
        )

        def read_pod_side_effect(pod):
            assert pod == pod_mock
            read_pod_side_effect.call_counter += 1
            if read_pod_side_effect.call_counter == 1:
                return remote_pod_mock_1
            else:
                return remote_pod_mock_2

        read_pod_side_effect.call_counter = 0
        self_mock = mock.Mock(read_pod=mock.Mock(side_effect=read_pod_side_effect))

        _composer_fetch_container_logs(mock.Mock())(self_mock, pod=pod_mock)

        stream_peer_vm_logs_mock.assert_called_once_with(
            self_mock,
            pod=pod_mock,
            client=logging_service_v2_client_mock(),
            project_id="test-project",
            peer_vm_name="standard-micro-123",
            since_timestamp="2023-05-02T10:11:12Z",
            insert_id="",
        )

    @mock.patch("airflow.composer.kubernetes.pod_manager._stream_peer_vm_logs", autospec=True)
    def test_composer_fetch_container_logs_peer_vm_no_annotation(self, stream_peer_vm_logs_mock):
        pod_mock = mock.Mock()
        container_mock = mock.Mock()
        container_mock.name = "peervm-placeholder"
        remote_pod_mock = mock.Mock()
        remote_pod_mock.spec = mock.Mock(containers=[container_mock])
        remote_pod_mock.status = mock.Mock(phase="Failed")
        remote_pod_mock.metadata = mock.Mock(annotations={})

        def read_pod_side_effect(pod):
            assert pod == pod_mock
            return remote_pod_mock

        self_mock = mock.Mock(read_pod=mock.Mock(side_effect=read_pod_side_effect))

        _composer_fetch_container_logs(mock.Mock())(self_mock, pod=pod_mock)

        stream_peer_vm_logs_mock.assert_not_called()

    @mock.patch("airflow.composer.kubernetes.pod_manager.time.sleep", autospec=True)
    def test_stream_peer_vm_logs(self, time_sleep_mock):
        pod_mock = mock.Mock()

        def container_is_running_side_effect(pod, container_name=None):
            assert pod == pod_mock
            assert container_name == "peervm-placeholder"
            container_is_running_side_effect.call_counter += 1
            if container_is_running_side_effect.call_counter == 1:
                return True
            else:
                return False

        container_is_running_side_effect.call_counter = 0
        self_mock = mock.Mock(container_is_running=mock.Mock(side_effect=container_is_running_side_effect))

        def list_log_entries_side_effect(request=None):
            list_log_entries_side_effect.call_counter += 1
            assert list_log_entries_side_effect.call_counter <= 2
            if list_log_entries_side_effect.call_counter == 1:
                assert request == ListLogEntriesRequest(
                    resource_names=["projects/test-project"],
                    filter="\n".join(
                        [
                            'resource.type="k8s_container"',
                            'resource.labels.project_id="test-project"',
                            'labels.peervm_name="standard-micro-abc"',
                            '(timestamp>"2023-05-02T10:11:12Z" OR '
                            '(timestamp="2023-05-02T10:11:12Z" AND insert_id>""))',
                        ]
                    ),
                    order_by="timestamp asc",
                    page_size=1000,
                )
                return mock.Mock(
                    __iter__=lambda self: iter(
                        [
                            LogEntry(
                                timestamp=Timestamp(seconds=1683022784), insert_id="qwe", text_payload="A"
                            ),
                            LogEntry(
                                timestamp=Timestamp(seconds=1683022785), insert_id="abc", text_payload="B"
                            ),
                        ]
                    )
                )
            else:
                assert request == ListLogEntriesRequest(
                    resource_names=["projects/test-project"],
                    filter="\n".join(
                        [
                            'resource.type="k8s_container"',
                            'resource.labels.project_id="test-project"',
                            'labels.peervm_name="standard-micro-abc"',
                            '(timestamp>"2023-05-02T10:19:45.000000Z" OR '
                            '(timestamp="2023-05-02T10:19:45.000000Z" AND insert_id>"abc"))',
                        ]
                    ),
                    order_by="timestamp asc",
                    page_size=1000,
                )
                return mock.Mock(
                    __iter__=lambda self: iter(
                        [
                            LogEntry(
                                timestamp=Timestamp(seconds=1683022786), insert_id="xyz", text_payload="C"
                            ),
                        ]
                    )
                )

        list_log_entries_side_effect.call_counter = 0
        client_mock = mock.Mock(list_log_entries=mock.Mock(side_effect=list_log_entries_side_effect))

        _stream_peer_vm_logs(
            self_mock,
            pod=pod_mock,
            client=client_mock,
            project_id="test-project",
            peer_vm_name="standard-micro-abc",
            since_timestamp="2023-05-02T10:11:12Z",
            insert_id="",
        )

        time_sleep_mock.assert_has_calls(
            [
                mock.call(15),
                mock.call(15),
            ]
        )
        assert self_mock.log.info.call_args_list == [
            mock.call("A"),
            mock.call("B"),
            mock.call("C"),
        ]
