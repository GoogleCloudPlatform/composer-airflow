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
from unittest import mock

import pytest

from airflow.composer.kubernetes.pod_manager import (
    _composer_fetch_container_logs,
    _composer_get_container_names,
    _stream_peer_vm_logs,
    patch_fetch_container_logs,
)
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager


class TestPodManager:
    @mock.patch("airflow.composer.kubernetes.pod_manager._composer_fetch_container_logs", autospec=True)
    @mock.patch("airflow.composer.kubernetes.pod_manager._composer_get_container_names", autospec=True)
    def test_patch_fetch_container_logs(
        self, composer_get_container_names_mock, composer_fetch_container_logs_mock
    ):
        # test setUp
        PodManager.fetch_container_logs._composer_patched = False

        # Call twice to check patching occurres only once.
        patch_fetch_container_logs()
        patch_fetch_container_logs()

        composer_fetch_container_logs_mock.assert_called_once()
        assert getattr(PodManager.fetch_container_logs, "_composer_patched") is True
        composer_get_container_names_mock.assert_called_once()

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

        def f_mock_side_effect(self, pod=None, container_name=None):
            assert self == self_mock
            assert pod == pod_mock
            assert container_name == "base"
            return result_mock

        f_mock = mock.Mock(side_effect=f_mock_side_effect)

        result = _composer_fetch_container_logs(f_mock)(self_mock, pod=pod_mock, container_name="base")

        assert result == result_mock

    @mock.patch("airflow.composer.kubernetes.pod_manager._stream_peer_vm_logs", autospec=True)
    @mock.patch.dict("os.environ", {"GCP_TENANT_PROJECT": "test-project"})
    def test_composer_fetch_container_logs_peer_vm(self, stream_peer_vm_logs_mock):
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
            annotations={"node.gke.io/peer-vm-endpoint": "10.5.3.2"},
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

        _composer_fetch_container_logs(mock.Mock())(self_mock, pod=pod_mock, container_name="base")

        stream_peer_vm_logs_mock.assert_called_once_with(
            self_mock,
            pod=pod_mock,
            container_name="base",
            peer_vm_endpoint="10.5.3.2",
            after_timestamp="2023-05-02T10:11:12.0Z",
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

        _composer_fetch_container_logs(mock.Mock())(self_mock, pod=pod_mock, container_name="base")

        stream_peer_vm_logs_mock.assert_not_called()

    @mock.patch("airflow.composer.kubernetes.pod_manager.time.sleep", autospec=True)
    @mock.patch("airflow.composer.kubernetes.pod_manager.requests", autospec=True)
    def test_stream_peer_vm_logs(self, requests_mock, time_sleep_mock):
        pod_mock = mock.Mock()

        def container_is_running_side_effect(pod, container_name=None):
            assert pod == pod_mock
            assert container_name == "peervm-placeholder"
            container_is_running_side_effect.call_counter += 1
            if container_is_running_side_effect.call_counter <= 4:
                return True
            else:
                return False

        container_is_running_side_effect.call_counter = 0
        self_mock = mock.Mock(container_is_running=mock.Mock(side_effect=container_is_running_side_effect))

        def requests_get_side_effect(url, params=None):
            requests_get_side_effect.call_counter += 1
            assert requests_get_side_effect.call_counter <= 5
            if requests_get_side_effect.call_counter == 1:
                assert url == "http://10.5.0.1:9080/logs"
                assert params == {
                    "container_name": "base",
                    "after_timestamp": "2023-05-02T10:11:12.0Z",
                    "max_log_lines": 1000,
                }
                return mock.Mock(status_code=500)
            elif requests_get_side_effect.call_counter == 2:
                assert url == "http://10.5.0.1:9080/logs"
                assert params == {
                    "container_name": "base",
                    "after_timestamp": "2023-05-02T10:11:12.0Z",
                    "max_log_lines": 1000,
                }
                return mock.Mock(
                    status_code=200,
                    json=mock.Mock(
                        return_value={
                            "logs": [
                                "2023-05-02T10:11:12.0Z stdout F A",
                                "2023-05-02T10:11:12.1Z stdout F B",
                                "2023-05-02T10:11:12.2Z stdout F C",
                            ]
                        }
                    ),
                )
            elif requests_get_side_effect.call_counter == 3:
                assert url == "http://10.5.0.1:9080/logs"
                assert params == {
                    "container_name": "base",
                    "after_timestamp": "2023-05-02T10:11:12.2Z",
                    "max_log_lines": 1000,
                }
                return mock.Mock(
                    status_code=200,
                    json=mock.Mock(
                        return_value={
                            "logs": [
                                "2023-05-02T10:11:15.5Z stdout F D",
                            ]
                        }
                    ),
                )
            elif requests_get_side_effect.call_counter == 4:
                assert url == "http://10.5.0.1:9080/logs"
                assert params == {
                    "container_name": "base",
                    "after_timestamp": "2023-05-02T10:11:15.5Z",
                    "max_log_lines": 1000,
                }
                return mock.Mock(status_code=200, json=mock.Mock(return_value={"logs": None}))
            elif requests_get_side_effect.call_counter == 5:
                raise Exception("Connection refused")

        requests_get_side_effect.call_counter = 0
        requests_mock.get = mock.Mock(side_effect=requests_get_side_effect)

        _stream_peer_vm_logs(
            self_mock,
            pod=pod_mock,
            container_name="base",
            peer_vm_endpoint="10.5.0.1",
            after_timestamp="2023-05-02T10:11:12.0Z",
        )

        time_sleep_mock.assert_has_calls(
            [
                mock.call(1),
                mock.call(1),
                mock.call(1),
                mock.call(1),
                mock.call(1),
            ]
        )
        assert self_mock.log.info.call_args_list == [
            mock.call("A"),
            mock.call("B"),
            mock.call("C"),
            mock.call("D"),
        ]

    @pytest.mark.parametrize(
        "original_container_names, expected_container_names",
        [
            (["base", "sidecar"], ["base", "sidecar"]),
            (["peervm-placeholder"], ["base"]),
        ],
    )
    def test_composer_get_container_names(self, original_container_names, expected_container_names):
        actual_container_names = _composer_get_container_names(lambda _: original_container_names)(
            mock.Mock()
        )

        assert actual_container_names == expected_container_names
