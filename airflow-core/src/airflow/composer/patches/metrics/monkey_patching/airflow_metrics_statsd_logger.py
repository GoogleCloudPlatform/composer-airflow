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

import functools

from airflow._shared.observability.metrics.statsd_logger import SafeStatsdLogger
from airflow.utils.net import get_hostname


def patch():
    SafeStatsdLogger.timer = _composer_statsd_logger_timer(SafeStatsdLogger.timer)


def _composer_statsd_logger_timer(f):
    @functools.wraps(f)
    def wrapper(self, stat=None, *args, **kwargs):
        if stat == "scheduler.scheduler_loop_duration":
            stat = f"scheduler.scheduler_loop_duration.{get_hostname()}"
        return f(self, stat, *args, **kwargs)

    return wrapper
