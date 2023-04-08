# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock

import pendulum
import pytest
import time_machine
from kubernetes.client.rest import ApiException
from pendulum import DateTime
from pendulum.tz.timezone import Timezone
from urllib3.exceptions import HTTPError as BaseHTTPError

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    PodLogsConsumer,
    PodManager,
    PodPhase,
    container_is_running,
    container_is_terminated,
)
from airflow.utils.timezone import utc


class TestPodManager:
    def setup_method(self):
        self.mock_kube_client = mock.Mock()
        self.pod_manager = PodManager(kube_client=self.mock_kube_client)

    def test_read_pod_logs_successfully_returns_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock.sentinel.logs
        logs = self.pod_manager.read_pod_logs(pod=mock.sentinel, container_name="base")
        assert type(logs) == PodLogsConsumer
        assert logs.response == mock.sentinel.logs

    def test_read_pod_logs_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError("Boom"),
            mock.sentinel.logs,
        ]
        logs = self.pod_manager.read_pod_logs(pod=mock.sentinel, container_name="base")
        assert type(logs) == PodLogsConsumer
        assert mock.sentinel.logs == logs.response
        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls(
            [
                mock.call(
                    _preload_content=False,
                    container="base",
                    follow=True,
                    timestamps=False,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                ),
                mock.call(
                    _preload_content=False,
                    container="base",
                    follow=True,
                    timestamps=False,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                ),
            ]
        )

    def test_read_pod_logs_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
        ]
        with pytest.raises(BaseHTTPError):
            self.pod_manager.read_pod_logs(pod=mock.sentinel, container_name="base")

    def test_read_pod_logs_successfully_with_tail_lines(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [mock.sentinel.logs]
        logs = self.pod_manager.read_pod_logs(pod=mock.sentinel, container_name="base", tail_lines=100)
        assert type(logs) == PodLogsConsumer
        assert mock.sentinel.logs == logs.response
        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls(
            [
                mock.call(
                    _preload_content=False,
                    container="base",
                    follow=True,
                    timestamps=False,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                    tail_lines=100,
                ),
            ]
        )

    def test_read_pod_logs_successfully_with_since_seconds(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [mock.sentinel.logs]
        logs = self.pod_manager.read_pod_logs(mock.sentinel, "base", since_seconds=2)
        assert type(logs) == PodLogsConsumer
        assert mock.sentinel.logs == logs.response
        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls(
            [
                mock.call(
                    _preload_content=False,
                    container="base",
                    follow=True,
                    timestamps=False,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                    since_seconds=2,
                ),
            ]
        )

    def test_read_pod_events_successfully_returns_events(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.return_value = mock.sentinel.events
        events = self.pod_manager.read_pod_events(mock.sentinel)
        assert mock.sentinel.events == events

    def test_read_pod_events_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.side_effect = [
            BaseHTTPError("Boom"),
            mock.sentinel.events,
        ]
        events = self.pod_manager.read_pod_events(mock.sentinel)
        assert mock.sentinel.events == events
        self.mock_kube_client.list_namespaced_event.assert_has_calls(
            [
                mock.call(
                    namespace=mock.sentinel.metadata.namespace,
                    field_selector=f"involvedObject.name={mock.sentinel.metadata.name}",
                ),
                mock.call(
                    namespace=mock.sentinel.metadata.namespace,
                    field_selector=f"involvedObject.name={mock.sentinel.metadata.name}",
                ),
            ]
        )

    def test_read_pod_events_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.side_effect = [
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
        ]
        with pytest.raises(AirflowException):
            self.pod_manager.read_pod_events(mock.sentinel)

    def test_read_pod_returns_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.return_value = mock.sentinel.pod_info
        pod_info = self.pod_manager.read_pod(mock.sentinel)
        assert mock.sentinel.pod_info == pod_info

    def test_read_pod_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.side_effect = [
            BaseHTTPError("Boom"),
            mock.sentinel.pod_info,
        ]
        pod_info = self.pod_manager.read_pod(mock.sentinel)
        assert mock.sentinel.pod_info == pod_info
        self.mock_kube_client.read_namespaced_pod.assert_has_calls(
            [
                mock.call(mock.sentinel.metadata.name, mock.sentinel.metadata.namespace),
                mock.call(mock.sentinel.metadata.name, mock.sentinel.metadata.namespace),
            ]
        )

    def test_monitor_pod_empty_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        running_status = mock.MagicMock()
        running_status.configure_mock(**{"name": "base", "state.running": True})
        pod_info_running = mock.MagicMock(**{"status.container_statuses": [running_status]})
        pod_info_succeeded = mock.MagicMock(**{"status.phase": PodPhase.SUCCEEDED})

        def pod_state_gen():
            yield pod_info_running
            while True:
                yield pod_info_succeeded

        self.mock_kube_client.read_namespaced_pod.side_effect = pod_state_gen()
        mock_response = mock.MagicMock(stream=mock.MagicMock(return_value=iter(())))
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock_response
        self.pod_manager.fetch_container_logs(mock.sentinel, "base")

    def test_monitor_pod_logs_failures_non_fatal(self):
        mock.sentinel.metadata = mock.MagicMock()
        running_status = mock.MagicMock()
        running_status.configure_mock(**{"name": "base", "state.running": True})
        pod_info_running = mock.MagicMock(**{"status.container_statuses": [running_status]})
        pod_info_succeeded = mock.MagicMock(**{"status.phase": PodPhase.SUCCEEDED})

        def pod_state_gen():
            yield pod_info_running
            yield pod_info_running
            while True:
                yield pod_info_succeeded

        self.mock_kube_client.read_namespaced_pod.side_effect = pod_state_gen()

        def pod_log_gen():
            while True:
                yield BaseHTTPError("Boom")

        self.mock_kube_client.read_namespaced_pod_log.side_effect = pod_log_gen()

        self.pod_manager.fetch_container_logs(mock.sentinel, "base")

    def test_read_pod_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.side_effect = [
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
        ]
        with pytest.raises(AirflowException):
            self.pod_manager.read_pod(mock.sentinel)

    def test_parse_log_line(self):
        log_message = "This should return no timestamp"
        timestamp, line = self.pod_manager.parse_log_line(log_message)
        assert timestamp is None
        assert line == log_message

        real_timestamp = "2020-10-08T14:16:17.793417674Z"
        timestamp, line = self.pod_manager.parse_log_line(" ".join([real_timestamp, log_message]))
        assert timestamp == pendulum.parse(real_timestamp)
        assert line == log_message

    def test_parse_invalid_log_line(self, caplog):
        with caplog.at_level(logging.INFO):
            self.pod_manager.parse_log_line("2020-10-08T14:16:17.793417674ZInvalidmessage\n")
        assert "Invalidmessage" in caplog.text
        assert "no timestamp in message" in caplog.text

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.run_pod_async")
    def test_start_pod_retries_on_409_error(self, mock_run_pod_async):
        mock_run_pod_async.side_effect = [
            ApiException(status=409),
            mock.MagicMock(),
        ]
        self.pod_manager.create_pod(mock.sentinel)
        assert mock_run_pod_async.call_count == 2

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.run_pod_async")
    def test_start_pod_fails_on_other_exception(self, mock_run_pod_async):
        mock_run_pod_async.side_effect = [ApiException(status=504)]
        with pytest.raises(ApiException):
            self.pod_manager.create_pod(mock.sentinel)

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.run_pod_async")
    def test_start_pod_retries_three_times(self, mock_run_pod_async):
        mock_run_pod_async.side_effect = [
            ApiException(status=409),
            ApiException(status=409),
            ApiException(status=409),
            ApiException(status=409),
        ]
        with pytest.raises(ApiException):
            self.pod_manager.create_pod(mock.sentinel)

        assert mock_run_pod_async.call_count == 3

    def test_start_pod_raises_informative_error_on_timeout(self):
        pod_response = mock.MagicMock()
        pod_response.status.phase = "Pending"
        self.mock_kube_client.read_namespaced_pod.return_value = pod_response
        expected_msg = "Check the pod events in kubernetes"
        mock_pod = MagicMock()
        with pytest.raises(AirflowException, match=expected_msg):
            self.pod_manager.await_pod_start(
                pod=mock_pod,
                startup_timeout=0,
            )

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running")
    def test_container_is_running(self, container_is_running_mock):
        mock_pod = MagicMock()
        self.pod_manager.read_pod = mock.MagicMock(return_value=mock_pod)
        self.pod_manager.container_is_running(None, "base")
        container_is_running_mock.assert_called_with(pod=mock_pod, container_name="base")

    @pytest.mark.parametrize("follow", [True, False])
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodLogsConsumer.logs_available")
    def test_fetch_container_done(self, logs_available, container_running, follow):
        """If container done, should exit, no matter setting of follow."""
        mock_pod = MagicMock()
        logs_available.return_value = False
        container_running.return_value = False
        ret = self.pod_manager.fetch_container_logs(pod=mock_pod, container_name="base", follow=follow)
        assert ret.last_log_time is None
        assert ret.running is False

    @mock.patch("pendulum.now")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodLogsConsumer.logs_available")
    def test_fetch_container_since_time(self, logs_available, container_running, mock_now):
        """If given since_time, should be used."""
        mock_pod = MagicMock()
        mock_now.return_value = DateTime(2020, 1, 1, 0, 0, 5, tzinfo=Timezone("UTC"))
        logs_available.return_value = True
        container_running.return_value = False
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock.MagicMock(
            stream=mock.MagicMock(return_value=[b"2021-01-01 hi"])
        )
        since_time = DateTime(2020, 1, 1, tzinfo=Timezone("UTC"))
        self.pod_manager.fetch_container_logs(pod=mock_pod, container_name="base", since_time=since_time)
        args, kwargs = self.mock_kube_client.read_namespaced_pod_log.call_args_list[0]
        assert kwargs["since_seconds"] == 5

    @pytest.mark.parametrize("follow, is_running_calls, exp_running", [(True, 3, False), (False, 3, False)])
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running")
    def test_fetch_container_running_follow(
        self, container_running_mock, follow, is_running_calls, exp_running
    ):
        """
        When called with follow, should keep looping even after disconnections, if pod still running.
        When called with follow=False, should return immediately even though still running.
        """
        mock_pod = MagicMock()
        container_running_mock.side_effect = [True, True, False]
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock.MagicMock(
            stream=mock.MagicMock(return_value=[b"2021-01-01 hi"])
        )
        ret = self.pod_manager.fetch_container_logs(pod=mock_pod, container_name="base", follow=follow)
        assert len(container_running_mock.call_args_list) == is_running_calls
        assert ret.last_log_time == DateTime(2021, 1, 1, tzinfo=Timezone("UTC"))
        assert ret.running is exp_running

    @pytest.mark.parametrize(
        "container_state, expected_is_terminated",
        [("waiting", False), ("running", False), ("terminated", True)],
    )
    def test_container_is_terminated_with_waiting_state(self, container_state, expected_is_terminated):
        container_status = MagicMock()
        container_status.configure_mock(
            **{
                "name": "base",
                "state.waiting": True if container_state == "waiting" else None,
                "state.running": True if container_state == "running" else None,
                "state.terminated": True if container_state == "terminated" else None,
            }
        )
        pod_info = MagicMock()
        pod_info.status.container_statuses = [container_status]
        assert container_is_terminated(pod_info, "base") == expected_is_terminated


def params_for_test_container_is_running():
    """The `container_is_running` method is designed to handle an assortment of bad objects
    returned from `read_pod`.  E.g. a None object, an object `e` such that `e.status` is None,
    an object `e` such that `e.status.container_statuses` is None, and so on.  This function
    emits params used in `test_container_is_running` to verify this behavior.

    We create mock classes not derived from MagicMock because with an instance `e` of MagicMock,
    tests like `e.hello is not None` are always True.
    """

    class RemotePodMock:
        pass

    class ContainerStatusMock:
        def __init__(self, name):
            self.name = name

    def remote_pod(running=None, not_running=None):
        e = RemotePodMock()
        e.status = RemotePodMock()
        e.status.container_statuses = []
        for r in not_running or []:
            e.status.container_statuses.append(container(r, False))
        for r in running or []:
            e.status.container_statuses.append(container(r, True))
        return e

    def container(name, running):
        c = ContainerStatusMock(name)
        c.state = RemotePodMock()
        c.state.running = {"a": "b"} if running else None
        return c

    pod_mock_list = []
    pod_mock_list.append(pytest.param(None, False, id="None remote_pod"))
    p = RemotePodMock()
    p.status = None
    pod_mock_list.append(pytest.param(p, False, id="None remote_pod.status"))
    p = RemotePodMock()
    p.status = RemotePodMock()
    p.status.container_statuses = []
    pod_mock_list.append(pytest.param(p, False, id="empty remote_pod.status.container_statuses"))
    pod_mock_list.append(pytest.param(remote_pod(), False, id="filter empty"))
    pod_mock_list.append(pytest.param(remote_pod(None, ["base"]), False, id="filter 0 running"))
    pod_mock_list.append(pytest.param(remote_pod(["hello"], ["base"]), False, id="filter 1 not running"))
    pod_mock_list.append(pytest.param(remote_pod(["base"], ["hello"]), True, id="filter 1 running"))
    return pod_mock_list


@pytest.mark.parametrize("remote_pod, result", params_for_test_container_is_running())
def test_container_is_running(remote_pod, result):
    """The `container_is_running` function is designed to handle an assortment of bad objects
    returned from `read_pod`.  E.g. a None object, an object `e` such that `e.status` is None,
    an object `e` such that `e.status.container_statuses` is None, and so on.  This test
    verifies the expected behavior."""
    assert container_is_running(remote_pod, "base") is result


class TestPodLogsConsumer:
    @pytest.mark.parametrize(
        "chunks, expected_logs",
        [
            ([b"message"], [b"message"]),
            ([b"message1\nmessage2"], [b"message1\n", b"message2"]),
            ([b"message1\n", b"message2"], [b"message1\n", b"message2"]),
            ([b"first_part", b"_second_part"], [b"first_part_second_part"]),
            ([b""], [b""]),
        ],
    )
    def test_chunks(self, chunks, expected_logs):
        with mock.patch.object(PodLogsConsumer, "logs_available") as logs_available:
            logs_available.return_value = True
            consumer = PodLogsConsumer(
                response=mock.MagicMock(stream=mock.MagicMock(return_value=chunks)),
                pod=mock.MagicMock(),
                pod_manager=mock.MagicMock(container_is_running=mock.MagicMock(return_value=True)),
                container_name="base",
            )
            assert list(consumer) == expected_logs

    def test_container_is_not_running(self):
        with mock.patch.object(PodLogsConsumer, "logs_available") as logs_available:
            logs_available.return_value = False
            consumer = PodLogsConsumer(
                response=mock.MagicMock(stream=mock.MagicMock(return_value=[b"message1", b"message2"])),
                pod=mock.MagicMock(),
                pod_manager=mock.MagicMock(container_is_running=mock.MagicMock(return_value=False)),
                container_name="base",
            )
            assert list(consumer) == []

    @pytest.mark.parametrize(
        "container_run, termination_time, now_time, post_termination_timeout, expected_logs_available",
        [
            (
                False,
                datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2022, 1, 1, 0, 1, 0, 0, tzinfo=utc),
                120,
                True,
            ),
            (
                False,
                datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2022, 1, 1, 0, 2, 0, 0, tzinfo=utc),
                120,
                False,
            ),
            (
                False,
                datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2022, 1, 1, 0, 5, 0, 0, tzinfo=utc),
                120,
                False,
            ),
            (
                True,
                datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2022, 1, 1, 0, 1, 0, 0, tzinfo=utc),
                120,
                True,
            ),
            (
                True,
                datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2022, 1, 1, 0, 2, 0, 0, tzinfo=utc),
                120,
                True,
            ),
            (
                True,
                datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2022, 1, 1, 0, 5, 0, 0, tzinfo=utc),
                120,
                True,
            ),
        ],
    )
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.get_container_status")
    def test_logs_available(
        self,
        mock_get_container_status,
        mock_container_is_running,
        container_run,
        termination_time,
        now_time,
        post_termination_timeout,
        expected_logs_available,
    ):
        mock_container_is_running.return_value = container_run
        mock_get_container_status.return_value = mock.MagicMock(
            state=mock.MagicMock(terminated=mock.MagicMock(finished_at=termination_time))
        )
        with time_machine.travel(now_time):
            consumer = PodLogsConsumer(
                response=mock.MagicMock(),
                pod=mock.MagicMock(),
                pod_manager=mock.MagicMock(),
                container_name="base",
                post_termination_timeout=post_termination_timeout,
            )
            assert consumer.logs_available() == expected_logs_available

    @pytest.mark.parametrize(
        "read_pod_cache_timeout, mock_read_pod_at_0, mock_read_pod_at_1, mock_read_pods, expected_read_pods",
        [
            (
                120,
                datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2023, 1, 1, 0, 1, 0, 0, tzinfo=utc),
                ["Read pod #0", "Read pod #1"],
                ["Read pod #0", "Read pod #0"],
            ),
            (
                120,
                datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2023, 1, 1, 0, 2, 0, 0, tzinfo=utc),
                ["Read pod #0", "Read pod #1"],
                ["Read pod #0", "Read pod #0"],
            ),
            (
                120,
                datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2023, 1, 1, 0, 3, 0, 0, tzinfo=utc),
                ["Read pod #0", "Read pod #1"],
                ["Read pod #0", "Read pod #1"],
            ),
            (
                2,
                datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2023, 1, 1, 0, 0, 1, 0, tzinfo=utc),
                ["Read pod #0", "Read pod #1"],
                ["Read pod #0", "Read pod #0"],
            ),
            (
                2,
                datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2023, 1, 1, 0, 0, 2, 0, tzinfo=utc),
                ["Read pod #0", "Read pod #1"],
                ["Read pod #0", "Read pod #0"],
            ),
            (
                2,
                datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2023, 1, 1, 0, 0, 3, 0, tzinfo=utc),
                ["Read pod #0", "Read pod #1"],
                ["Read pod #0", "Read pod #1"],
            ),
        ],
    )
    def test_read_pod(
        self,
        read_pod_cache_timeout,
        mock_read_pod_at_0,
        mock_read_pod_at_1,
        mock_read_pods,
        expected_read_pods,
    ):
        consumer = PodLogsConsumer(
            response=mock.MagicMock(),
            pod=mock.MagicMock(),
            pod_manager=mock.MagicMock(),
            container_name="base",
            read_pod_cache_timeout=read_pod_cache_timeout,
        )
        consumer.pod_manager.read_pod.side_effect = mock_read_pods
        # first read
        with time_machine.travel(mock_read_pod_at_0):
            assert consumer.read_pod() == expected_read_pods[0]

        # second read
        with time_machine.travel(mock_read_pod_at_1):
            assert consumer.read_pod() == expected_read_pods[1]
