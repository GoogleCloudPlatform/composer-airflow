#
# Copyright 2025 Google LLC
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

import signal
from datetime import timedelta
from unittest import mock

import pytest
from sqlalchemy import text

from airflow._shared.timezones import timezone
from airflow.composer.patches.database_retention.trim import (
    _run_trimming_loop,
    _sigalrm_handler,
    _sigint_handler,
    execute_trim,
)
from airflow.jobs.job import Job
from airflow.models import (
    DagRun,
    Log,
    RenderedTaskInstanceFields,
    TaskInstance,
)
from airflow.models.errors import ParseImportError
from airflow.models.xcom import XComModel
from airflow.utils.session import provide_session


class TestTrim:
    @provide_session
    def test_execute_trim_session_table(self, session):
        # It is not easy to access `session` model, thus we are using raw queries here.
        session.execute(
            text("DELETE FROM session")
        )  # Drop the whole table, as it anyway shouldn't be used across the tests.
        session.execute(text("INSERT INTO session(id, expiry) VALUES(1, '1900-01-03 07:28:41.405961')"))
        session.execute(text("INSERT INTO session(id, expiry) VALUES(2, '2024-01-03 07:28:41.405961')"))
        session.execute(text("INSERT INTO session(id, expiry) VALUES(3, '2135-01-03 07:28:41.405961')"))
        session.commit()

        session_ids = session.execute(text("SELECT id FROM session WHERE id in (1, 2, 3) ORDER BY id")).all()
        assert session_ids == [(1,), (2,), (3,)]

        execute_trim(30, batch_size=100, sleep_between_batches_seconds=0)

        session_ids = session.execute(text("SELECT id FROM session WHERE id in (1, 2, 3) ORDER BY id")).all()
        assert session_ids == [(3,)]

        # Clean up table after test execution.
        session.execute(text("DELETE FROM session"))
        session.commit()

    @provide_session
    def test_execute_trim_job_table(self, session):
        retention_days = 30
        utcnow = timezone.utcnow()
        # Drop the whole table, as it anyway shouldn't be used across the tests.
        session.query(Job).delete()
        for ind, latest_heartbeat in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            session.add(Job(id=ind + 1, latest_heartbeat=latest_heartbeat))
        session.commit()

        job_ids = session.query(Job.id).filter(Job.id.in_([1, 2, 3])).all()
        assert job_ids == [(1,), (2,), (3,)]

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        job_ids = session.query(Job.id).filter(Job.id.in_([1, 2, 3])).all()
        assert job_ids == [(3,)]

    @provide_session
    def test_execute_trim_log_table(self, session):
        retention_days = 30
        utcnow = timezone.utcnow()
        for ind, dttm in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            log = Log(event=f"event_{ind}")
            log.id = ind + 1
            log.dttm = dttm
            session.add(log)
        session.commit()

        log_ids = session.query(Log.id).filter(Log.id.in_([1, 2, 3])).all()
        assert log_ids == [(1,), (2,), (3,)]

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        log_ids = session.query(Log.id).filter(Log.id.in_([1, 2, 3])).all()
        assert log_ids == [(3,)]

    @provide_session
    def test_execute_trim_parse_import_error_table(self, session):
        retention_days = 30
        utcnow = timezone.utcnow()
        for ind, timestamp in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            parse_import_error = ParseImportError(
                id=ind + 1,
                timestamp=timestamp,
            )
            session.add(parse_import_error)
        session.commit()

        parse_import_error_ids = (
            session.query(ParseImportError.id).filter(ParseImportError.id.in_([1, 2, 3])).all()
        )
        assert parse_import_error_ids == [(1,), (2,), (3,)]

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        parse_import_error_ids = (
            session.query(ParseImportError.id).filter(ParseImportError.id.in_([1, 2, 3])).all()
        )
        assert parse_import_error_ids == [(3,)]

    @provide_session
    def test_execute_trim_xcom_table(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()

        # Drop the whole table, as it anyway shouldn't be used across the tests.
        session.query(XComModel).delete()
        for ind, logical_date in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            ti = create_task_instance(run_id=f"run_id_{ind}")

            ti.dag_run.logical_date = logical_date
            xcom = XComModel(
                dag_run_id=ti.dag_run.id,
                task_id=ti.task_id,
                map_index=ti.map_index,
                key="key",
                dag_id=ti.dag_id,
                run_id=ti.run_id,
            )
            session.add(ti)
            session.add(xcom)
        session.commit()

        assert session.query(XComModel).count() == 3

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        assert session.query(XComModel).count() == 1
        assert session.query(XComModel).first().logical_date == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)

    @provide_session
    def test_execute_trim_rendered_task_instance_fields_table(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()

        # Drop the whole table, as it anyway shouldn't be used across the tests.
        session.query(RenderedTaskInstanceFields).delete()
        for ind, logical_date in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            ti = create_task_instance(run_id=f"trim_rendered_task_instance_fields_{ind}")

            ti.dag_run.logical_date = logical_date
            rendered_task_instance_fields = RenderedTaskInstanceFields(ti=ti)
            session.add(ti)
            session.add(rendered_task_instance_fields)
        session.commit()

        assert session.query(RenderedTaskInstanceFields).count() == 3

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        assert session.query(RenderedTaskInstanceFields).count() == 1
        assert session.query(RenderedTaskInstanceFields).first().logical_date[0] == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)

    @provide_session
    def test_execute_trim_task_instance_dag_run_tables(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()

        # Drop tables, as they anyway shouldn't be used across the tests.
        session.query(TaskInstance).delete()
        session.query(DagRun).delete()
        for ind, logical_date in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            ti = create_task_instance(run_id=f"trim_task_instance_dag_run_{ind}")

            ti.dag_run.logical_date = logical_date
            session.add(ti)
        session.commit()

        assert session.query(TaskInstance).count() == 3
        assert session.query(DagRun).count() == 3

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        assert session.query(TaskInstance).count() == 1
        assert session.query(DagRun).count() == 1
        assert session.query(TaskInstance).first().logical_date == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)
        assert session.query(DagRun).first().logical_date == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)

    @provide_session
    def test_execute_trim_task_instance_dag_run_tables_keep_last(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()

        # Drop tables, as they anyway shouldn't be used across the tests.
        session.query(TaskInstance).delete()
        session.query(DagRun).delete()
        for ind, logical_date in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=5),
            ]
        ):
            ti = create_task_instance(run_id=f"trim_task_instance_dag_run_keep_last_{ind}")

            ti.dag_run.logical_date = logical_date
            session.add(ti)
        session.commit()

        assert session.query(TaskInstance).count() == 3
        assert session.query(DagRun).count() == 3

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        assert session.query(TaskInstance).count() == 0
        assert session.query(DagRun).count() == 1
        assert session.query(DagRun).first().logical_date == utcnow - timedelta(
            days=retention_days
        ) - timedelta(seconds=5)

    @mock.patch("signal.signal", autospec=True)
    def test_execute_trim_signals(self, signal_mock):
        execute_trim(30, batch_size=100, sleep_between_batches_seconds=0)

        signal_mock.assert_has_calls(
            [
                mock.call(signal.SIGINT, _sigint_handler),
                mock.call(signal.SIGALRM, _sigalrm_handler),
            ]
        )

    @mock.patch("airflow.composer.patches.database_retention.trim._trim_session_table", autospec=True)
    def test_execute_trim_exception(self, trim_session_table_mock):
        trim_session_table_mock.side_effect = ValueError("cleanup failed")

        with pytest.raises(SystemExit) as exc:
            execute_trim(30, batch_size=100, sleep_between_batches_seconds=0)

        assert exc.value.code == 1

    @provide_session
    def test_execute_trim_batches(self, session):
        retention_days = 30
        utcnow = timezone.utcnow()
        # Drop the whole table, as it anyway shouldn't be used across the tests.
        session.query(Job).delete()
        for ind, latest_heartbeat in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=5),
            ]
        ):
            session.add(Job(id=ind + 1, latest_heartbeat=latest_heartbeat))
        session.commit()

        job_ids = session.query(Job.id).all()
        assert job_ids == [(1,), (2,), (3,)]

        execute_trim(retention_days, batch_size=1, sleep_between_batches_seconds=0)

        job_ids = session.query(Job.id).all()
        assert job_ids == []

    def test_run_trimming_loop_retry_successful(self):
        def session_mock_commit_side_effect():
            session_mock_commit_side_effect.call_counter += 1
            if session_mock_commit_side_effect.call_counter == 1:
                raise Exception("Commit exception")

        session_mock_commit_side_effect.call_counter = 0
        session_mock = mock.MagicMock()
        session_mock.commit.side_effect = session_mock_commit_side_effect

        trim_batch_func = mock.MagicMock()
        trim_batch_func.side_effect = [
            10,
            10,
            0,
        ]

        _run_trimming_loop(session_mock, "test-table", trim_batch_func, 0)

        session_mock.rollback.assert_called_once_with()

    def test_run_trimming_loop_retry_unsuccessful(self):
        session_mock = mock.MagicMock()
        session_mock.commit.side_effect = ValueError("Commit error")
        trim_batch_func = mock.MagicMock()
        trim_batch_func.return_value = 10

        with pytest.raises(ValueError) as exc:
            _run_trimming_loop(session_mock, "test-table", trim_batch_func, 0)

        assert isinstance(exc.value, ValueError)
        assert str(exc.value) == "Commit error"
        assert session_mock.rollback.call_args_list == [mock.call(), mock.call(), mock.call()]

    def test_sigint_handler(self):
        with pytest.raises(SystemExit) as exc:
            _sigint_handler("signum", "frame")

        assert isinstance(exc.value, SystemExit)
        assert exc.value.code == 0

    def test_sigalrm_handler(self):
        with pytest.raises(SystemExit) as exc:
            _sigalrm_handler("signum", "frame")

        assert isinstance(exc.value, SystemExit)
        assert exc.value.code == 0
