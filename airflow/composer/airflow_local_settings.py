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
def setup_logging_on_celeryd_init(**kwargs):
    """Customize Celery logging configuration as per Composer needs."""
    from airflow.configuration import conf

    # Apply same format for Celery logs as we have for all other logs.
    # From https://github.com/celery/celery/issues/3599.
    kwargs["conf"].worker_log_format = conf.get("logging", "LOG_FORMAT")

    # Set broker_connection_retry_on_startup as broker_connection_retry to suppress CPendingDeprecationWarning
    # coming from Celery:
    # "The broker_connection_retry configuration setting will no longer determine whether broker connection
    # retries are made during startup in Celery 6.0 and above. If you wish to retain the existing behavior for
    # retrying connections on startup, you should set broker_connection_retry_on_startup to True."
    kwargs["conf"].broker_connection_retry_on_startup = kwargs["conf"].broker_connection_retry
