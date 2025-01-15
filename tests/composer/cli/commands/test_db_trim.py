#
# Copyright 2023 Google LLC
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

import datetime
import os
from unittest import mock

import pytest
from sqlalchemy import text

from airflow.cli import cli_parser
from airflow.composer.cli.commands import db_command
from airflow.composer.db_command.db_trim import (
    Config,
    execute_trim,
    trim_session_table,
    trim_table,
    run_trimming_loop,
    MAX_RETRY_ATTEMPTS,
)
from airflow.composer.db_command.tables_to_trim import tables_to_trim
from airflow.utils.session import create_session
from airflow.utils.timezone import make_aware


def prepare_tables():
    from airflow.jobs.job import Job
    from airflow.models import (
        DagRun,
        ImportError,
        Log,
        RenderedTaskInstanceFields,
        SlaMiss,
        TaskInstance,
        TaskReschedule,
        XCom,
    )

    return [
        {"model": Job, "age_column": Job.latest_heartbeat},
        {"model": Log, "age_column": Log.dttm},
        {"model": SlaMiss, "age_column": SlaMiss.execution_date},
        {"model": ImportError, "age_column": ImportError.timestamp},
        {"model": XCom, "age_column": XCom.execution_date},
        {"model": RenderedTaskInstanceFields, "age_column": RenderedTaskInstanceFields.execution_date},
        {"model": TaskReschedule, "age_column": TaskReschedule.execution_date},
        {"model": TaskInstance, "age_column": TaskInstance.execution_date},
        {"model": DagRun, "age_column": DagRun.execution_date},
    ]


def count_tables_rows(tables, trim_execute_time):
    count_tables = {}
    with create_session() as session:
        for table in tables:
            count_tables[table["model"].__tablename__] = (
                session.query(table["model"])
                .filter(table["age_column"] is not None, table["age_column"] < trim_execute_time)
                .count()
            )
    return count_tables


def execute_sql_file(sql_file):
    with create_session() as session:
        for line in sql_file:
            if len(line) > 1:
                session.execute(text(line))


test_tables = prepare_tables()


class TestDbTrim:
    CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))

    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @pytest.mark.parametrize(
        "tables,extra_args", [(test_tables, ["--retention-days", "730", "--acknowledge-composer-internal"])]
    )
    def test_e2e_db_trim(self, tables, extra_args):
        trim_execute_time = make_aware(datetime.datetime(year=2000, month=1, day=1))

        before_count_tables = count_tables_rows(tables, trim_execute_time)

        with open(
            os.path.join(self.CURRENT_DIRECTORY, "../../test_data/db_trim_inserts.sql")
        ) as insert_sql_file:
            execute_sql_file(insert_sql_file)

        args = self.parser.parse_args(
            [
                "db",
                "trim",
                *extra_args,
            ]
        )
        db_command.trim(args)

        after_count_tables = count_tables_rows(tables, trim_execute_time)

        with open(
            os.path.join(self.CURRENT_DIRECTORY, "../../test_data/db_trim_deletes.sql")
        ) as deletes_sql_file:
            execute_sql_file(deletes_sql_file)

        for key in before_count_tables.keys():
            if key == "dag_run":
                assert before_count_tables[key] + 2 == after_count_tables[key]
            else:
                assert before_count_tables[key] == after_count_tables[key]

    def test_tables_to_trim_order(self):
        """Checking the order of tables per Airflow version

        This test should ensure us that we didn't change the order of tables
        in test_tables_to_trim_order function. We want to ensure that code is handling
        all of supported versions properly, since it might vary by version."""
        tables = tables_to_trim()
        expected_order = prepare_tables()
        assert len(expected_order) == len(tables)
        for i in range(len(expected_order)):
            assert expected_order[i]["model"].__tablename__ == tables[i]["airflow_db_model"].__tablename__

    @pytest.mark.parametrize(
        "retention_days",
        [(30), (100), (730)],
    )
    @mock.patch("airflow.composer.cli.commands.db_command.trim")
    def test_cli_db_trim_within_range_success(self, mock_db_trim, retention_days):
        args = self.parser.parse_args(
            [
                "db",
                "trim",
                "--retention-days",
                f"{retention_days}",
                "--acknowledge-composer-internal",
            ]
        )
        db_command.trim(args)
        mock_db_trim.assert_called_once_with(args)

    @pytest.mark.parametrize(
        "retention_days",
        [(-5), (13), (1000)],
    )
    def test_cli_db_trim_within_range_failure(self, retention_days):
        args = self.parser.parse_args(
            [
                "db",
                "trim",
                "--retention-days",
                f"{retention_days}",
                "--acknowledge-composer-internal",
            ]
        )
        with pytest.raises(ValueError) as value_exception:
            db_command.trim(args)

        assert "Retention horizon must be in range(30, 730)" in str(value_exception.value)

    @mock.patch("airflow.composer.db_command.db_trim.trim_session_table")
    @mock.patch("airflow.composer.db_command.db_trim.trim_table")
    def test_execute_trim_calls_trimming_once(self, mock_table_trim, mock_session_trim):
        execute_trim(retention_days=1000)

        assert mock_table_trim.call_count == len(test_tables)
        assert mock_session_trim.call_count == 1

    def test_run_trimming_loop_calls_trimming_until_no_expired_rows_are_left(self):
        session = mock.Mock(["commit", "rollback"])
        trim_batch = mock.Mock()
        trim_batch.side_effect = [1000, 1000, 50, 0]

        run_trimming_loop(
            session=session,
            table_name="fake_table",
            estimated_num_expired_rows=3000,
            trim_batch_func=trim_batch,
        )

        assert trim_batch.call_count == 4

    def test_run_trimming_loop_retries_after_an_exception(self):
        session = mock.Mock(["commit", "rollback"])
        session.commit.side_effect = [Exception("fake deadlock"), None]
        trim_batch = mock.Mock()
        trim_batch.side_effect = [1000, 1000, 0]

        run_trimming_loop(
            session=session,
            table_name="fake_table",
            estimated_num_expired_rows=2000,
            trim_batch_func=trim_batch,
        )

        assert trim_batch.call_count == 3
        # We should call .rollback() once, when handling an exception.
        assert session.rollback.call_count == 1
        # We should call .commit() every time except for at the end, when no expired rows are left.
        assert session.commit.call_count == 2

    def test_run_trimming_loop_stops_retrying_after_reaching_max_attempts(self):
        session = mock.Mock(["commit", "rollback"])
        session.commit.side_effect = [Exception("fake deadlock")] * MAX_RETRY_ATTEMPTS
        trim_batch = mock.Mock()
        trim_batch.side_effect = [1000] * MAX_RETRY_ATTEMPTS

        with pytest.raises(Exception) as final_exception:
            run_trimming_loop(
                session=session,
                table_name="fake_table",
                estimated_num_expired_rows=2000,
                trim_batch_func=trim_batch,
            )

        assert "fake deadlock" in str(final_exception.value)
        assert trim_batch.call_count == MAX_RETRY_ATTEMPTS
        assert session.rollback.call_count == MAX_RETRY_ATTEMPTS
        assert session.commit.call_count == MAX_RETRY_ATTEMPTS
