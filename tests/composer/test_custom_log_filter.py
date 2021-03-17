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
import logging
import unittest

from airflow.composer.custom_log_filter import ComposerFilter


class TestComposerFilter(unittest.TestCase):

    def test_detecting_redis_warning(self):
        record = logging.LogRecord('logger_name', logging.WARN,
                                   'default_celery.py', 80,
                                   'You have configured a result_backend of redis://airflow-redis-service:6379/0, it is highly recommended to use an alternative result_backend (i.e. a database).',
                                   None, None)

        self.assertFalse(ComposerFilter().filter(record))

    def test_example_message_not_filtered_out(self):
        record = logging.LogRecord('logger_name', logging.INFO,
                                   'dagbag.py', 418,
                                   'Filling up the DagBag from /home/airflow/gcs/dags/airflow_monitoring.py',
                                   None, None)

        self.assertTrue(ComposerFilter().filter(record))
