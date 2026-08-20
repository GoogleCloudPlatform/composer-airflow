#
# Copyright 2026 Google LLC
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

from unittest import mock

from airflow._shared.observability.metrics.statsd_logger import SafeStatsdLogger
from airflow.composer.patches.metrics.monkey_patching.airflow_metrics_statsd_logger import patch


class TestAirflowMetricsStatsdLogger:
    @mock.patch("airflow.composer.patches.metrics.monkey_patching.airflow_metrics_statsd_logger.get_hostname")
    def test_patch(self, get_hostname_mock):
        get_hostname_mock.return_value = "airflow-scheduler-xyz"
        mock_statsd_client = mock.MagicMock()
        logger = SafeStatsdLogger(statsd_client=mock_statsd_client)

        patch()

        logger.timer("scheduler.scheduler_loop_duration")
        mock_statsd_client.timer.assert_called_once_with(
            "scheduler.scheduler_loop_duration.airflow-scheduler-xyz"
        )

    @mock.patch("airflow.composer.patches.metrics.monkey_patching.airflow_metrics_statsd_logger.get_hostname")
    def test_patch_other_stat(self, get_hostname_mock):
        get_hostname_mock.return_value = "airflow-scheduler-xyz"
        mock_statsd_client = mock.MagicMock()
        logger = SafeStatsdLogger(statsd_client=mock_statsd_client)

        patch()

        logger.timer("other.metric")
        mock_statsd_client.timer.assert_called_once_with("other.metric")
