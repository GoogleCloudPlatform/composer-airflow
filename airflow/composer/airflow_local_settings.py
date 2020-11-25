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
"""Airflow local settings."""

from celery.signals import celeryd_init


@celeryd_init.connect
def setup_log_format(**kwargs):
    """Apply same format for Celery logs as we have for all other logs.

    From https://github.com/celery/celery/issues/3599.
    """
    from airflow.configuration import conf

    kwargs["conf"].worker_log_format = conf.get("logging", "LOG_FORMAT")
