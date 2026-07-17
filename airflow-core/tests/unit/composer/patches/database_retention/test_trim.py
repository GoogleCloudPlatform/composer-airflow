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

import os
import signal
from datetime import timedelta
from unittest import mock

import pytest
from sqlalchemy import delete, func as sqlfunc, select, text

from airflow._shared.timezones import timezone
from airflow.composer.patches.database_retention.trim import (
    _count_and_log_num_rows,
    _run_trimming_loop,
    _sigalrm_handler,
    _sigint_handler,
    execute_trim,
)
from airflow.jobs.job import Job
from airflow.models import (
    DagRun,
    Deadline,
    HITLDetail,
    Log,
    RenderedTaskInstanceFields,
    TaskInstance,
    TaskReschedule,
)
from airflow.models.asset import AssetEvent
from airflow.models.backfill import Backfill, BackfillDagRun
from airflow.models.errors import ParseImportError
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.models.xcom import XComModel
from airflow.sdk.definitions.deadline import AsyncCallback


class TestTrim:
    @mock.patch.dict(os.environ, {"AIRFLOW3_DATABASE_RETENTION_NEW_TABLES": "True"})
    @mock.patch("airflow.composer.patches.database_retention.trim.logger")
    def test_execute_trim_session_table(self, mock_logger, session):
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

        # Verify that the row estimation was logged correctly
        mock_logger.info.assert_any_call(
            "Airflow metadata cleanup calculated number of expired rows to remove for table '%s': %s",
            "session",
            "2",
        )

        # Clean up table after test execution.
        session.execute(text("DELETE FROM session"))
        session.commit()

    @mock.patch.dict(os.environ, {"AIRFLOW3_DATABASE_RETENTION_NEW_TABLES": "True"})
    @mock.patch("airflow.composer.patches.database_retention.trim.logger")
    def test_execute_trim_job_table(self, mock_logger, session):
        retention_days = 30
        utcnow = timezone.utcnow()
        # Drop the whole table, as it anyway shouldn't be used across the tests.
        session.execute(delete(Job))
        for ind, latest_heartbeat in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            session.add(Job(id=ind + 1, latest_heartbeat=latest_heartbeat))
        session.commit()

        job_ids = session.execute(select(Job.id).filter(Job.id.in_([1, 2, 3]))).all()
        assert job_ids == [(1,), (2,), (3,)]

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        job_ids = session.execute(select(Job.id).filter(Job.id.in_([1, 2, 3]))).all()
        assert job_ids == [(3,)]

        # Verify that the row estimation was logged correctly
        mock_logger.info.assert_any_call(
            "Airflow metadata cleanup calculated number of expired rows to remove for table '%s': %s",
            "job",
            "2",
        )

    def test_execute_trim_log_table(self, session):
        retention_days = 30
        utcnow = timezone.utcnow()
        # Drop the whole table, as it anyway shouldn't be used across the tests.
        session.execute(delete(Log))
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

        log_ids = session.execute(select(Log.id).filter(Log.id.in_([1, 2, 3]))).all()
        assert log_ids == [(1,), (2,), (3,)]

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        log_ids = session.execute(select(Log.id).filter(Log.id.in_([1, 2, 3]))).all()
        assert log_ids == [(3,)]

    def test_execute_trim_parse_import_error_table(self, session):
        retention_days = 30
        utcnow = timezone.utcnow()
        # Drop the whole table, as it anyway shouldn't be used across the tests.
        session.execute(delete(ParseImportError))
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

        parse_import_error_ids = session.execute(
            select(ParseImportError.id).filter(ParseImportError.id.in_([1, 2, 3]))
        ).all()
        assert parse_import_error_ids == [(1,), (2,), (3,)]

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        parse_import_error_ids = session.execute(
            select(ParseImportError.id).filter(ParseImportError.id.in_([1, 2, 3]))
        ).all()
        assert parse_import_error_ids == [(3,)]

    def test_execute_trim_xcom_table(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()

        # Drop the whole table, as it anyway shouldn't be used across the tests.
        session.execute(delete(XComModel))
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

        assert session.scalar(select(sqlfunc.count()).select_from(XComModel)) == 3

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        assert session.scalar(select(sqlfunc.count()).select_from(XComModel)) == 1
        assert session.scalars(select(XComModel)).first().logical_date == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)

    def test_execute_trim_rendered_task_instance_fields_table(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()

        # Drop the whole table, as it anyway shouldn't be used across the tests.
        session.execute(delete(RenderedTaskInstanceFields))
        for ind, logical_date in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            ti = create_task_instance(run_id=f"trim_rendered_task_instance_fields_{ind}")

            ti.dag_run.logical_date = logical_date
            rendered_task_instance_fields = RenderedTaskInstanceFields(ti=ti, render_templates=False)
            session.add(ti)
            session.add(rendered_task_instance_fields)
        session.commit()

        assert session.scalar(select(sqlfunc.count()).select_from(RenderedTaskInstanceFields)) == 3

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        assert session.scalar(select(sqlfunc.count()).select_from(RenderedTaskInstanceFields)) == 1
        assert session.scalars(select(RenderedTaskInstanceFields)).first().logical_date[
            0
        ] == utcnow - timedelta(days=retention_days) + timedelta(seconds=10)

    def test_execute_trim_task_instance_dag_run_tables(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()

        # Drop tables, as they anyway shouldn't be used across the tests.
        session.execute(delete(TaskInstance))
        session.execute(delete(DagRun))
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

        assert session.scalar(select(sqlfunc.count()).select_from(TaskInstance)) == 3
        assert session.scalar(select(sqlfunc.count()).select_from(DagRun)) == 3

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        assert session.scalar(select(sqlfunc.count()).select_from(TaskInstance)) == 1
        assert session.scalar(select(sqlfunc.count()).select_from(DagRun)) == 1
        assert session.scalars(select(TaskInstance)).first().logical_date == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)
        assert session.scalars(select(DagRun)).first().logical_date == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)

    def test_execute_trim_task_instance_dag_run_tables_keep_last(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()

        # Drop tables, as they anyway shouldn't be used across the tests.
        session.execute(delete(TaskInstance))
        session.execute(delete(DagRun))
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

        assert session.scalar(select(sqlfunc.count()).select_from(TaskInstance)) == 3
        assert session.scalar(select(sqlfunc.count()).select_from(DagRun)) == 3

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        assert session.scalar(select(sqlfunc.count()).select_from(TaskInstance)) == 0
        assert session.scalar(select(sqlfunc.count()).select_from(DagRun)) == 1
        assert session.scalars(select(DagRun)).first().logical_date == utcnow - timedelta(
            days=retention_days
        ) - timedelta(seconds=5)

    def test_execute_trim_task_instance_dag_run_tables_logical_date_null(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()

        # Drop tables, as they anyway shouldn't be used across the tests.
        session.execute(delete(TaskInstance))
        session.execute(delete(DagRun))
        for ind, logical_date in enumerate(
            [
                None,
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            ti = create_task_instance(run_id=f"trim_task_instance_dag_run_{ind}")

            ti.dag_run.logical_date = logical_date
            session.add(ti)
        session.commit()

        assert session.scalar(select(sqlfunc.count()).select_from(TaskInstance)) == 4
        assert session.scalar(select(sqlfunc.count()).select_from(DagRun)) == 4

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        assert session.scalar(select(sqlfunc.count()).select_from(TaskInstance)) == 2
        assert session.scalar(select(sqlfunc.count()).select_from(DagRun)) == 2
        assert set(ti.logical_date for ti in session.scalars(select(TaskInstance)).all()) == {
            None,
            utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
        }
        assert set(ti.logical_date for ti in session.scalars(select(DagRun)).all()) == {
            None,
            utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
        }

    @mock.patch.dict(os.environ, {"AIRFLOW3_DATABASE_RETENTION_NEW_TABLES": "True"})
    def test_execute_trim_asset_event_table(self, session):
        retention_days = 30
        utcnow = timezone.utcnow()
        session.execute(delete(AssetEvent))
        for timestamp in [
            utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
            utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
            utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
        ]:
            event = AssetEvent(asset_id=1, extra={})
            event.timestamp = timestamp
            session.add(event)
        session.commit()

        assert session.scalar(select(sqlfunc.count()).select_from(AssetEvent)) == 3
        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)
        assert session.scalar(select(sqlfunc.count()).select_from(AssetEvent)) == 1
        assert session.scalars(select(AssetEvent)).first().timestamp == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)

    @mock.patch.dict(os.environ, {"AIRFLOW3_DATABASE_RETENTION_NEW_TABLES": "True"})
    def test_execute_trim_task_reschedule_table(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()
        session.execute(delete(TaskReschedule))
        for ind, logical_date in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            ti = create_task_instance(run_id=f"run_id_reschedule_{ind}")
            ti.dag_run.logical_date = logical_date
            tr = TaskReschedule(
                ti_id=ti.id,
                start_date=utcnow - timedelta(days=retention_days) - timedelta(seconds=2000),
                end_date=utcnow - timedelta(days=retention_days) - timedelta(seconds=1500),
                reschedule_date=logical_date,
            )
            session.add(ti)
            session.add(tr)
        session.commit()

        assert session.scalar(select(sqlfunc.count()).select_from(TaskReschedule)) == 3
        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)
        assert session.scalar(select(sqlfunc.count()).select_from(TaskReschedule)) == 1
        assert session.scalars(select(TaskReschedule)).first().reschedule_date == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)

    @mock.patch.dict(os.environ, {"AIRFLOW3_DATABASE_RETENTION_NEW_TABLES": "True"})
    def test_execute_trim_task_instance_history_table(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()
        session.execute(delete(TaskInstanceHistory))
        for ind, logical_date in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            ti = create_task_instance(run_id=f"run_id_history_{ind}")
            ti.dag_run.logical_date = logical_date
            ti.end_date = logical_date
            tih = TaskInstanceHistory(ti=ti, state="success")
            session.add(ti)
            session.add(tih)
        session.commit()

        assert session.scalar(select(sqlfunc.count()).select_from(TaskInstanceHistory)) == 3
        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)
        assert session.scalar(select(sqlfunc.count()).select_from(TaskInstanceHistory)) == 1
        assert session.scalars(select(TaskInstanceHistory)).first().end_date == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)

    @mock.patch.dict(os.environ, {"AIRFLOW3_DATABASE_RETENTION_NEW_TABLES": "True"})
    def test_execute_trim_hitl_detail_table(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()
        session.execute(delete(HITLDetail))
        for ind, logical_date in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            ti = create_task_instance(run_id=f"run_id_hitl_{ind}")
            ti.dag_run.logical_date = logical_date
            hitl = HITLDetail(
                ti_id=ti.id,
                options={"o": "k"},
                subject="subj",
                created_at=logical_date,
            )
            session.add(ti)
            session.add(hitl)
        session.commit()

        assert session.scalar(select(sqlfunc.count()).select_from(HITLDetail)) == 3
        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)
        assert session.scalar(select(sqlfunc.count()).select_from(HITLDetail)) == 1
        assert session.scalars(select(HITLDetail)).first().created_at == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)

    @mock.patch.dict(os.environ, {"AIRFLOW3_DATABASE_RETENTION_NEW_TABLES": "True"})
    def test_execute_trim_deadline_table(self, session, create_task_instance):

        async def dummy_callback():
            pass

        retention_days = 30
        utcnow = timezone.utcnow()
        session.execute(delete(Deadline))
        for ind, logical_date in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            ti = create_task_instance(run_id=f"run_id_deadline_{ind}")
            ti.dag_run.logical_date = logical_date
            callback = AsyncCallback(dummy_callback)
            deadline = Deadline(
                deadline_time=logical_date,
                callback=callback,
                dagrun_id=ti.dag_run.id,
                deadline_alert_id=None,
            )
            session.add(ti)
            session.add(deadline)
        session.commit()

        assert session.scalar(select(sqlfunc.count()).select_from(Deadline)) == 3
        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)
        assert session.scalar(select(sqlfunc.count()).select_from(Deadline)) == 1
        assert session.scalars(select(Deadline)).first().deadline_time == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)

    @mock.patch.dict(os.environ, {"AIRFLOW3_DATABASE_RETENTION_NEW_TABLES": "True"})
    def test_execute_trim_backfill_tables(self, session, create_task_instance):
        retention_days = 30
        utcnow = timezone.utcnow()
        session.execute(delete(BackfillDagRun))
        session.execute(delete(Backfill))
        for ind, date in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) + timedelta(seconds=10),
            ]
        ):
            backfill = Backfill(
                dag_id="dag",
                from_date=utcnow,
                to_date=utcnow,
                created_at=date,
            )
            session.add(backfill)
            session.flush()

            ti = create_task_instance(run_id=f"run_id_backfill_{ind}")
            ti.dag_run.logical_date = date

            bdr = BackfillDagRun(
                backfill_id=backfill.id,
                dag_run_id=ti.dag_run.id,
                logical_date=date,
                sort_ordinal=1,
            )
            session.add(ti)
            session.add(bdr)
        session.commit()

        assert session.scalar(select(sqlfunc.count()).select_from(Backfill)) == 3
        assert session.scalar(select(sqlfunc.count()).select_from(BackfillDagRun)) == 3

        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)

        assert session.scalar(select(sqlfunc.count()).select_from(Backfill)) == 1
        assert session.scalar(select(sqlfunc.count()).select_from(BackfillDagRun)) == 1
        assert session.scalars(select(Backfill)).first().created_at == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)
        assert session.scalars(select(BackfillDagRun)).first().logical_date == utcnow - timedelta(
            days=retention_days
        ) + timedelta(seconds=10)

    @mock.patch.dict(os.environ, {"AIRFLOW3_DATABASE_RETENTION_NEW_TABLES": "False"})
    def test_execute_trim_new_tables_skipped_when_flag_disabled(self, session):
        retention_days = 30
        utcnow = timezone.utcnow()
        session.execute(delete(AssetEvent))
        for timestamp in [
            utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
            utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
        ]:
            event = AssetEvent(asset_id=1, extra={})
            event.timestamp = timestamp
            session.add(event)
        session.commit()

        assert session.scalar(select(sqlfunc.count()).select_from(AssetEvent)) == 2
        execute_trim(retention_days, batch_size=100, sleep_between_batches_seconds=0)
        assert session.scalar(select(sqlfunc.count()).select_from(AssetEvent)) == 2

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

    def test_execute_trim_batches(self, session):
        retention_days = 30
        utcnow = timezone.utcnow()
        # Drop the whole table, as it anyway shouldn't be used across the tests.
        session.execute(delete(Job))
        for ind, latest_heartbeat in enumerate(
            [
                utcnow - timedelta(days=retention_days) - timedelta(seconds=1000),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=10),
                utcnow - timedelta(days=retention_days) - timedelta(seconds=5),
            ]
        ):
            session.add(Job(id=ind + 1, latest_heartbeat=latest_heartbeat))
        session.commit()

        job_ids = session.execute(select(Job.id)).all()
        assert job_ids == [(1,), (2,), (3,)]

        execute_trim(retention_days, batch_size=1, sleep_between_batches_seconds=0)

        job_ids = session.execute(select(Job.id)).all()
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

        with pytest.raises(ValueError, match="Commit error") as exc:
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

    @mock.patch("airflow.composer.patches.database_retention.trim.logger")
    def test_count_and_log_num_rows_exception(self, mock_logger):
        error = RuntimeError("DB error")
        count_func_mock = mock.MagicMock(side_effect=error)

        _count_and_log_num_rows("test_table", count_func_mock)

        mock_logger.warning.assert_called_once_with(
            "Airflow metadata cleanup failed to calculate number of expired rows to remove for table '%s'. Error: %s",
            "test_table",
            error,
        )
        mock_logger.info.assert_not_called()
