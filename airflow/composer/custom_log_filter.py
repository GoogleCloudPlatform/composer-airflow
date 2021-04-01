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
"""Custom log filter for Cloud Composer."""
import logging
import os
import re

PROVIDERS_HOOK_MISSING_ATTRIBUTE_WARNING_RE = re.compile(
    r"The '<class 'airflow.providers[a-zA-Z\.]*Hook'>' is missing [a-z_]* attribute and cannot be registered"
)


def _is_redis_warning(record):
    """Method that detects using Redis as result backend warning."""
    return record.getMessage().startswith('You have configured a result_backend of redis://')


def _is_stats_client_warning(record):
    """Method that detects stats client is not configured warning."""
    return record.getMessage().startswith('Could not configure StatsClient: ')


def _is_providers_hook_missing_attribute_warning(record):
    """Method that detects missing attribute warning for provider hooks."""
    return PROVIDERS_HOOK_MISSING_ATTRIBUTE_WARNING_RE.match(record.getMessage())


class ComposerFilter(logging.Filter):
    """Custom Composer log filter."""

    def filter(self, record):
        # The concern with running Redis backend is that celery task messages may get
        # lost across Redis restarts. Composer has provisioned Redis service using
        # StatefulSet and saves a snapshot every 60 seconds to a persistent disk.
        # So it's not a concern to use the Redis backend in Cloud Composer.
        # From https://groups.google.com/g/cloud-composer-discuss/c/8SY2NdjjOS4
        if _is_redis_warning(record):
            return False

        # Webserver doesn't have access to statsd host, and it doesn't need because it doesn't
        # send any metrics, therefore we can safely silent this message for webserver.
        if _is_stats_client_warning(record) and os.environ.get('AIRFLOW_WEBSERVER', None) == 'True':
            return False

        # Warning about missing attribute for hook doesn't mean this hook is not usable,
        # it means that this hook will not be available e.g. on "Connections" page in UI.
        # In case of custom user hooks this warning is useful, in case of provider hooks
        # this warning is useless for Airflow user because they are not able to fix it.
        if _is_providers_hook_missing_attribute_warning(record):
            return False

        return True
