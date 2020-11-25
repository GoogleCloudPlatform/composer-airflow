#
# Copyright 2022 Google LLC
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
import copy
import io
import logging
import unittest

from airflow.config_templates import airflow_local_settings
from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from tests.test_utils.db import clear_db_runs

TEST_TASK_FORMATTER_CONFIG = copy.deepcopy(airflow_local_settings.DEFAULT_LOGGING_CONFIG)
TEST_TASK_FORMATTER_CONFIG['handlers']['task_console']['stream'] = io.StringIO()


class TestTaskFormatter(unittest.TestCase):
    def setUp(self):
        logging.config.dictConfig(TEST_TASK_FORMATTER_CONFIG)
        date = datetime(2020, 1, 1)
        self.dag = DAG('dag_for_testing_composer_task_formatter', start_date=date)
        self.dag.create_dagrun(
            state=State.SUCCESS, run_id='test_run_id', execution_date=date, data_interval=(date, date)
        )
        self.addCleanup(self.dag.clear)
        self.task = DummyOperator(task_id='task_for_testing_composer_task_formatter', dag=self.dag)
        self.ti = TaskInstance(task=self.task, run_id='test_run_id')
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        self.ti.get_dagrun()
        self.stream = TEST_TASK_FORMATTER_CONFIG['handlers']['task_console']['stream']
        self.stream.truncate(0)
        self.stream.seek(0)

    def tearDown(self):
        clear_db_runs()

    def test_appends_metadata(self):
        self.ti.init_run_context()
        self.ti.log.info('sample-message')
        self.assertRegex(
            self.stream.getvalue(),
            '.*INFO - sample-message@-@{'
            + '"workflow": "dag_for_testing_composer_task_formatter", '
            + '"task-id": "task_for_testing_composer_task_formatter", '
            + r'"execution-date": "2020-01-01T00:00:00\+00:00", '
            + '"try-number": "1"}\n',
        )

    def test_handles_missing_metadata(self):
        self.ti.log.info('sample-message')
        self.assertRegex(self.stream.getvalue(), '.*INFO - sample-message$')

    def test_appends_metadata_to_exception(self):
        self.ti.init_run_context()
        try:
            raise AssertionError()
        except AssertionError:
            self.ti.log.exception('sample-exception')

        self.assertRegex(
            self.stream.getvalue(),
            '.*ERROR - sample-exception@-@{'
            + '"workflow": "dag_for_testing_composer_task_formatter", '
            + '"task-id": "task_for_testing_composer_task_formatter", '
            + r'"execution-date": "2020-01-01T00:00:00\+00:00", '
            + '"try-number": "1"}\n'
            + 'Traceback(?s).*'
            + 'AssertionError@-@{"workflow": "dag_for_testing_composer_task_formatter", '
            + '"task-id": "task_for_testing_composer_task_formatter", '
            + '"execution-date": "2020-01-01T00:00:00\\+00:00", "try-number": "1"}\n',
        )
