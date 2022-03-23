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
import contextlib
import importlib
import io
import logging
import unittest
import warnings
from unittest import mock

from airflow.composer import custom_log_filter
from airflow.logging_config import configure_logging


class TestComposerFilter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        configure_logging()

    def test_detecting_redis_warning(self):
        logger = logging.getLogger('airflow.config_templates.default_celery')
        message = (
            'You have configured a result_backend of redis://airflow-redis'
            '-service:6379/0, it is highly recommended to use an alternative '
            'result_backend (i.e. a database).'
        )
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.warning(message)

        self.assertNotIn(message, temp_stdout.getvalue())

    def test_detecting_stats_client_warning(self):
        logger = logging.getLogger('airflow.stats')
        message = (
            'Could not configure StatsClient: [Errno -2] Name or service not '
            'known, using DummyStatsLogger instead.'
        )

        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.error(message)
        self.assertIn(message, temp_stdout.getvalue())

        with mock.patch('os.environ', {'AIRFLOW_WEBSERVER': 'True'}):
            with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
                logger.error(message)
        self.assertNotIn(message, temp_stdout.getvalue())

    def test_detecting_refused_to_delete_permission_view_warning(self):
        logger = logging.getLogger('airflow.www.fab_security')
        message = 'Refused to delete permission view, assoc with role exists DAG Runs.can_create User'

        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.warning(message)
        self.assertNotIn(message, temp_stdout.getvalue())

    def test_detecting_providers_hook_missing_attribute_warning(self):
        logger = logging.getLogger('airflow.providers_manager')
        message = (
            "The '<class 'airflow.providers.apache.beam.hooks.beam.BeamHook'>' "
            "is missing conn_name_attr attribute and cannot be registered"
        )
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.warning(message)

        self.assertNotIn(message, temp_stdout.getvalue())

    def test_example_message_not_filtered_out(self):
        logger = logging.getLogger('airflow.settings')
        message = 'Filling up the DagBag from /home/airflow/gcs/dags/airflow_monitoring.py'
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            logger.info(message)

        self.assertIn(message, temp_stdout.getvalue())

    def test_ignoring_dag_concurrency_option_renamed_warning(self):
        # Reload custom_log_filter to apply Composer filters for warning module.
        # They are removed (presumably by pytest) before running test.
        importlib.reload(custom_log_filter)

        message = (
            "The dag_concurrency option in [core] has been renamed to max_active_tasks_per_dag "
            "- the old setting has been used, but please update your config"
        )
        with warnings.catch_warnings(record=True) as warning_list:
            warnings.warn(message, DeprecationWarning, stacklevel=3)

        warning_list = [w.message.args[0] for w in warning_list]
        self.assertNotIn(message, warning_list)
