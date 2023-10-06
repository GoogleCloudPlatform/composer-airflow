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

import contextlib
import io
import logging
import subprocess
from unittest import mock

import pytest

from airflow.logging_config import configure_logging


class TestComposerFilter:
    @classmethod
    def setup_class(cls):
        configure_logging()

    def test_detecting_redis_warning(self):
        logger = logging.getLogger("airflow.config_templates.default_celery")
        message = (
            "You have configured a result_backend of redis://airflow-redis"
            "-service:6379/0, it is highly recommended to use an alternative "
            "result_backend (i.e. a database)."
        )
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.warning(message)

        assert message not in temp_stdout.getvalue()

    def test_detecting_stats_client_warning(self):
        logger = logging.getLogger("airflow.stats")
        message = (
            "Could not configure StatsClient: [Errno -2] Name or service not "
            "known, using DummyStatsLogger instead."
        )

        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.error(message)
        assert message in temp_stdout.getvalue()

        with mock.patch("os.environ", {"AIRFLOW_WEBSERVER": "True"}):
            with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
                logger.error(message)
        assert message not in temp_stdout.getvalue()

    def test_detecting_refused_to_delete_permission_view_warning(self):
        logger = logging.getLogger("airflow.www.fab_security")
        message = "Refused to delete permission view, assoc with role exists DAG Runs.can_create User"

        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.warning(message)
        assert message not in temp_stdout.getvalue()

    def test_detecting_no_user_yet_created_warning(self):
        logger = logging.getLogger("airflow.www.fab_security")
        message = "No user yet created, use flask fab command to do it."

        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.warning(message)
        assert message not in temp_stdout.getvalue()

    def test_detecting_providers_hook_missing_attribute_warning(self):
        logger = logging.getLogger("airflow.providers_manager")
        message = (
            "The '<class 'airflow.providers.apache.beam.hooks.beam.BeamHook'>' "
            "is missing conn_name_attr attribute and cannot be registered"
        )
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.warning(message)

        assert message not in temp_stdout.getvalue()

    def test_detecting_duplicate_key_value_in_permission_tables_warning(self):
        logger = logging.getLogger("airflow.www.fab_security")
        message = (
            "Add Permission to Role Error: (psycopg2.errors.UniqueViolation) duplicate "
            "key value violates unique constraint 'ab_role_name_key'"
        )
        with mock.patch("os.environ", {"AIRFLOW_WEBSERVER": "True"}):
            with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
                logger.error(message)

        assert message not in temp_stdout.getvalue()

    def test_example_message_not_filtered_out(self):
        logger = logging.getLogger("airflow.settings")
        message = "Filling up the DagBag from /home/airflow/gcs/dags/airflow_monitoring.py"
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.info(message)

        assert message in temp_stdout.getvalue()

    @pytest.mark.parametrize(
        "flower_warning",
        [
            "Inspect method stats failed",
            "Inspect method scheduled failed",
            "Inspect method registered failed",
            "Inspect method active_queues failed",
            "Inspect method revoked failed",
            "Inspect method conf failed",
            "Inspect method active failed",
            "Inspect method reserved failed",
        ],
    )
    def test_ignoring_flower_warnings(self, flower_warning):
        logger = logging.getLogger("flower.inspector")
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.warning(flower_warning)

        assert flower_warning not in temp_stdout.getvalue()

    def test_ignoring_celery_deprecation_warning(self):
        celery_warning = (
            "CPendingDeprecationWarning: The broker_connection_retry configuration setting "
            "will no longer determine whether broker connection retries are made during startup in "
            "Celery 6.0 and above. If you wish to retain the existing behavior for retrying "
            "connections on startup, you should set broker_connection_retry_on_startup to True."
        )
        logger = logging.getLogger("celery.worker")
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.warning(celery_warning)

        assert celery_warning not in temp_stdout.getvalue()

    @pytest.mark.parametrize(
        "section, key, deprecated_key, expected",
        [
            ("core", "max_active_tasks_per_dag", "dag_concurrency", False),
            ("api", "auth_backends", "auth_backend", False),
            ("scheduler", "parsing_processes", "max_threads", True),
        ],
    )
    def test_configuration_option_renamed_warnings(self, section, key, deprecated_key, expected):
        output = subprocess.check_output(
            [
                "python",
                "-c",
                (f"from airflow.configuration import conf; print(conf.get('{section}', '{key}'))"),
            ],
            env={f"AIRFLOW__{section.upper()}__{deprecated_key.upper()}": ""},
            stderr=subprocess.STDOUT,
        ).decode()

        message = f"{deprecated_key} option in [{section}] has been renamed to {key}"
        assert (message in output) is expected, f"{message} in output is expected: f{expected}"

    def test_ignoring_type_decorator_cache_warning(self):
        message = (
            "TypeDecorator JSONField() will not produce a cache key because the ``cache_ok`` flag "
            "is not set to True. Set this flag to True if this type object's state is safe to use "
            "in a cache key, or False to disable this warning."
        )

        output = subprocess.check_output(
            [
                "python",
                "-c",
                (
                    "import airflow, warnings, sqlalchemy.exc; "
                    # We should use here double quotes around message (as it is used above) to have proper
                    # python string formatting.
                    f'warnings.warn("{message}", sqlalchemy.exc.SAWarning, 3)'
                ),
            ],
            stderr=subprocess.STDOUT,
        ).decode()

        assert message not in output
