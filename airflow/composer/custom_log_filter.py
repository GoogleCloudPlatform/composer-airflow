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


def _is_refused_to_delete_permission_view_warning(record):
    """Method that detects refused to delete permission view warning."""
    return record.getMessage().startswith('Refused to delete permission view, assoc with role exists ')


def _is_no_user_yet_created_warning(record):
    """Method that detects no user yet created warning."""
    return record.getMessage() == 'No user yet created, use flask fab command to do it.'


def _is_providers_hook_missing_attribute_warning(record):
    """Method that detects missing attribute warning for provider hooks."""
    return PROVIDERS_HOOK_MISSING_ATTRIBUTE_WARNING_RE.match(record.getMessage())


def _is_duplicate_key_value_in_permision_tables_warning(record):
    """Method that detects postgres warning for violating unique constraint in permission tables"""
    constraints = [
        'ab_permission_view_permission_id_view_menu_id_key',
        'ab_permission_view_role_permission_view_id_role_id_key',
        'ab_view_menu_name_key', 'ab_permission_name_key', 'ab_role_name_key'
    ]
    record_message = record.getMessage()
    return ('psycopg2.errors.UniqueViolation) duplicate key value violates '
            'unique constraint') in record_message and any([c in record_message for c in constraints])


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

        # According to https://github.com/apache/airflow/issues/10331#issuecomment-758108624
        # these warning messages are harmless and can be ignored.
        if _is_refused_to_delete_permission_view_warning(record):
            return False

        # This warning is printed on start up of webserver in case no users are yet registered
        # in Airflow RBAC. This message can be silent as it is not useful.
        if _is_no_user_yet_created_warning(record):
            return False

        # Warning about missing attribute for hook doesn't mean this hook is not usable,
        # it means that this hook will not be available e.g. on "Connections" page in UI.
        # In case of custom user hooks this warning is useful, in case of provider hooks
        # this warning is useless for Airflow user because they are not able to fix it.
        if _is_providers_hook_missing_attribute_warning(record):
            return False

        # These errors are known issue of Airflow 2.3.0-2.3.4 and they occur on webserver
        # startup but don't mean any malfunctioning and can be ignored.
        # https://github.com/apache/airflow/issues/23512
        if _is_duplicate_key_value_in_permision_tables_warning(record):
            return False

        return True
