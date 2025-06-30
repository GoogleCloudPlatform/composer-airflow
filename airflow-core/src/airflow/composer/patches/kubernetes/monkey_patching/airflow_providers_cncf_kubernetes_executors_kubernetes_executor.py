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

import functools
import time

from airflow.composer.patches.kubernetes.kubernetes_executor import (
    POD_TEMPLATE_FILE_REFRESH_INTERVAL,
    refresh_pod_template_file,
)
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor


def patch():
    KubernetesExecutor.start = _composer_kubernetes_executor_start(KubernetesExecutor.start)
    KubernetesExecutor.sync = _composer_kubernetes_executor_sync(KubernetesExecutor.sync)


def _composer_kubernetes_executor_start(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        result = f(self, *args, **kwargs)

        # Refresh Composer kubernetes pod template file on KubernetesExecutor start.
        self._composer_pod_template_file_timestamp = time.time()
        refresh_pod_template_file(self.kube_client.api_client)

        return result

    return wrapper


def _composer_kubernetes_executor_sync(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        result = f(self, *args, **kwargs)

        # Refresh Composer kubernetes pod template file periodically.
        if time.time() - self._composer_pod_template_file_timestamp > POD_TEMPLATE_FILE_REFRESH_INTERVAL:
            self._composer_pod_template_file_timestamp = time.time()
            refresh_pod_template_file(self.kube_client.api_client)

        return result

    return wrapper
