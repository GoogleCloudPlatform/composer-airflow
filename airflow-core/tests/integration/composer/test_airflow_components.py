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

import socket
from urllib.parse import urljoin

import pytest
import requests

from airflow.providers.celery.cli.celery_command import _check_if_active_celery_worker

from integration.composer.utils import API_SERVER_URL


class TestAirflowComponents:
    @pytest.mark.parametrize("component", ["metadatabase", "scheduler", "triggerer", "dag_processor"])
    def test_monitor_health(self, component):
        """This test verifies that scheduler, triggerer, Dag processor and API server started successfully."""
        monitor_health_url = urljoin(API_SERVER_URL, "/api/v2/monitor/health")

        response = requests.get(monitor_health_url)

        assert response.status_code == 200
        assert response.json()[component]["status"] == "healthy"

    def test_celery_worker_health(self):
        """This test verifies that Celery worker started successfully."""
        _check_if_active_celery_worker(f"celery@{socket.gethostname()}")
