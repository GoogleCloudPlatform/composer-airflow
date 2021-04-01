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
import io
import logging
import unittest
from unittest import mock

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
