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
from __future__ import annotations

import copy
import datetime as dt
import io
import logging

import re2

from airflow.config_templates import airflow_local_settings
from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from tests.test_utils.db import clear_db_runs

TEST_TASK_FORMATTER_CONFIG = copy.deepcopy(airflow_local_settings.DEFAULT_LOGGING_CONFIG)
TEST_TASK_FORMATTER_CONFIG["handlers"]["task_console"]["stream"] = io.StringIO()


def get_long_message():
    return "a" * 4096 + "b" * 4096 + "ccc"


class TestTaskFormatter:
    def setup_method(self):
        logging.config.dictConfig(TEST_TASK_FORMATTER_CONFIG)
        date = datetime(2020, 1, 1)
        self.dag = DAG(
            "dag_for_testing_composer_task_formatter",
            start_date=date,
            schedule=dt.timedelta(days=1),
        )
        self.dag.create_dagrun(
            state=State.SUCCESS, run_id="test_run_id", execution_date=date, data_interval=(date, date)
        )
        self.task = DummyOperator(task_id="task_for_testing_composer_task_formatter", dag=self.dag)
        self.ti = TaskInstance(task=self.task, run_id="test_run_id")
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        self.ti.get_dagrun()
        self.stream = TEST_TASK_FORMATTER_CONFIG["handlers"]["task_console"]["stream"]
        self.stream.truncate(0)
        self.stream.seek(0)

    def teardown_method(self):
        self.dag.clear
        clear_db_runs()

    def test_appends_metadata(self):
        self.ti.init_run_context()
        self.ti.log.info("sample-message")
        assert re2.match(
            ".*INFO - sample-message@-@{"
            '"workflow": "dag_for_testing_composer_task_formatter", '
            '"task-id": "task_for_testing_composer_task_formatter", '
            r'"execution-date": "2020-01-01T00:00:00\+00:00", '
            '"map-index": "-1", '
            '"try-number": "1"}\n',
            self.stream.getvalue(),
        )

    def test_handles_missing_metadata(self):
        self.ti.log.info("sample-message")
        assert re2.match(".*INFO - sample-message\n$", self.stream.getvalue())

    def test_appends_metadata_to_exception(self):
        self.ti.init_run_context()
        try:
            raise AssertionError()
        except AssertionError:
            self.ti.log.exception("sample-exception")

        assert re2.match(
            r"(?s:.*ERROR - sample-exception\\n"
            "Traceback.*"
            'AssertionError@-@{"workflow": "dag_for_testing_composer_task_formatter", '
            '"task-id": "task_for_testing_composer_task_formatter", '
            '"execution-date": "2020-01-01T00:00:00\\+00:00", '
            '"map-index": "-1", "try-number": "1"}\n)',
            self.stream.getvalue(),
        )

    def test_persists_esacaped_characters(self):
        self.ti.init_run_context()
        self.ti.log.info("message with \\n escape characters and \n new \r lines \t")

        assert re2.match(
            r".* INFO - message with \\\\n escape characters and \\n "
            r"new \\r lines \t@-@{"
            '"workflow": "dag_for_testing_composer_task_formatter", '
            '"task-id": "task_for_testing_composer_task_formatter", '
            r'"execution-date": "2020-01-01T00:00:00\+00:00", '
            '"map-index": "-1", '
            '"try-number": "1"}\n',
            self.stream.getvalue(),
        )

    def test_splits_and_appends_metadata_to_long_lines(self):
        self.ti.init_run_context()
        self.ti.log.info(get_long_message())

        value = self.stream.getvalue()
        lines = value.split("\n")
        assert len(lines) == 4
        assert lines[-1] == ""
        lines = lines[:-1]
        expected_annotation = (
            "@-@{"
            '"workflow": "dag_for_testing_composer_task_formatter", '
            '"task-id": "task_for_testing_composer_task_formatter", '
            r'"execution-date": "2020-01-01T00:00:00+00:00", '
            '"map-index": "-1", '
            '"try-number": "1"}'
        )
        sample_prefix = "[2024-11-21 15:41:52,400] {test_task_formatter.py:111} INFO - "
        for line in lines:
            assert len(line) <= 4096 + len(expected_annotation) + len(sample_prefix)
            assert re2.match(
                ".*@-@{"
                '"workflow": "dag_for_testing_composer_task_formatter", '
                '"task-id": "task_for_testing_composer_task_formatter", '
                r'"execution-date": "2020-01-01T00:00:00\+00:00", '
                '"map-index": "-1", '
                '"try-number": "1"}',
                line,
            )

    def test_persists_all_characters_in_split_lines(self):
        self.ti.init_run_context()
        self.ti.log.info(get_long_message())

        value = self.stream.getvalue()
        lines = value.split("\n")
        lines = lines[:-1]
        assert get_long_message() == "".join([line.split("@-@")[0].split("INFO - ")[-1] for line in lines])

    def test_extra_workflow_info(self):
        self.ti.init_run_context()
        self.ti.log.info("sample-message", extra={"extra_workflow_info": {"extra-label": "value"}})
        assert re2.match(
            ".*INFO - sample-message@-@{"
            '"workflow": "dag_for_testing_composer_task_formatter", '
            '"task-id": "task_for_testing_composer_task_formatter", '
            r'"execution-date": "2020-01-01T00:00:00\+00:00", '
            '"map-index": "-1", '
            '"try-number": "1", '
            '"extra-label": "value"}\n',
            self.stream.getvalue(),
        )

    def test_prefixes_split_lines_with_log_format(self):
        self.ti.init_run_context()
        self.ti.log.info(get_long_message())

        value = self.stream.getvalue()
        lines = value.split("\n")[:-1]

        for line in lines:
            assert re2.match("\[.*\] \{.*\} INFO - ", line)
