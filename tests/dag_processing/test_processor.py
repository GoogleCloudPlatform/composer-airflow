#
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
#

import datetime
import os
from datetime import timedelta
from tempfile import NamedTemporaryFile
from unittest import mock
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pytest

from airflow import settings
from airflow.configuration import conf
from airflow.dag_processing.processor import DagFileProcessor
from airflow.models import DAG, DagBag, DagModel, SlaMiss, TaskInstance, errors
from airflow.models.taskinstance import SimpleTaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone
from airflow.utils.callback_requests import TaskCallbackRequest
from airflow.utils.dates import days_ago
from airflow.utils.session import create_session
from airflow.utils.state import State
from tests.test_utils.config import conf_vars, env_vars
from tests.test_utils.db import (
    clear_db_dags,
    clear_db_import_errors,
    clear_db_jobs,
    clear_db_pools,
    clear_db_runs,
    clear_db_serialized_dags,
    clear_db_sla_miss,
)
from tests.test_utils.mock_executor import MockExecutor

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

# Include the words "airflow" and "dag" in the file contents,
# tricking airflow into thinking these
# files contain a DAG (otherwise Airflow will skip them)
PARSEABLE_DAG_FILE_CONTENTS = '"airflow DAG"'
UNPARSEABLE_DAG_FILE_CONTENTS = 'airflow DAG'
INVALID_DAG_WITH_DEPTH_FILE_CONTENTS = "def something():\n    return airflow_DAG\nsomething()"

# Filename to be used for dags that are created in an ad-hoc manner and can be removed/
# created at runtime
TEMP_DAG_FILENAME = "temp_dag.py"


@pytest.fixture(scope="class")
def disable_load_example():
    with conf_vars({('core', 'load_examples'): 'false'}):
        with env_vars({('core', 'load_examples'): 'false'}):
            yield


@pytest.mark.usefixtures("disable_load_example")
class TestDagFileProcessor:
    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_pools()
        clear_db_dags()
        clear_db_sla_miss()
        clear_db_import_errors()
        clear_db_jobs()
        clear_db_serialized_dags()

    def setup_method(self):
        self.clean_db()

        # Speed up some tests by not running the tasks, just look at what we
        # enqueue!
        self.null_exec = MockExecutor()
        self.scheduler_job = None

    def teardown_method(self) -> None:
        if self.scheduler_job and self.scheduler_job.processor_agent:
            self.scheduler_job.processor_agent.end()
            self.scheduler_job = None
        self.clean_db()

    def create_test_dag(self, start_date=DEFAULT_DATE, end_date=DEFAULT_DATE + timedelta(hours=1), **kwargs):
        dag = DAG(
            dag_id='test_scheduler_reschedule',
            start_date=start_date,
            # Make sure it only creates a single DAG Run
            end_date=end_date,
        )
        dag.clear()
        dag.is_subdag = False
        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id, is_paused=False)
            session.merge(orm_dag)
            session.commit()
        return dag

    @classmethod
    def setup_class(cls):
        # Ensure the DAGs we are looking at from the DB are up-to-date
        non_serialized_dagbag = DagBag(read_dags_from_db=False, include_examples=False)
        non_serialized_dagbag.sync_to_db()
        cls.dagbag = DagBag(read_dags_from_db=True)

    def _process_file(self, file_path, session):
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())

        dag_file_processor.process_file(file_path, [], False, session)

    def test_dag_file_processor_sla_miss_callback(self):
        """
        Test that the dag file processor calls the sla miss callback
        """
        session = settings.Session()

        sla_callback = MagicMock()

        # Create dag with a start of 1 day ago, but an sla of 0
        # so we'll already have an sla_miss on the books.
        test_start_date = days_ago(1)
        dag = DAG(
            dag_id='test_sla_miss',
            sla_miss_callback=sla_callback,
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta()},
        )

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='success'))

        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_file_processor.manage_slas(dag=dag, session=session)

        assert sla_callback.called

    def test_dag_file_processor_sla_miss_callback_invalid_sla(self):
        """
        Test that the dag file processor does not call the sla miss callback when
        given an invalid sla
        """
        session = settings.Session()

        sla_callback = MagicMock()

        # Create dag with a start of 1 day ago, but an sla of 0
        # so we'll already have an sla_miss on the books.
        # Pass anything besides a timedelta object to the sla argument.
        test_start_date = days_ago(1)
        dag = DAG(
            dag_id='test_sla_miss',
            sla_miss_callback=sla_callback,
            default_args={'start_date': test_start_date, 'sla': None},
        )

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='success'))

        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_file_processor.manage_slas(dag=dag, session=session)
        sla_callback.assert_not_called()

    def test_dag_file_processor_sla_miss_callback_sent_notification(self):
        """
        Test that the dag file processor does not call the sla_miss_callback when a
        notification has already been sent
        """
        session = settings.Session()

        # Mock the callback function so we can verify that it was not called
        sla_callback = MagicMock()

        # Create dag with a start of 2 days ago, but an sla of 1 day
        # ago so we'll already have an sla_miss on the books
        test_start_date = days_ago(2)
        dag = DAG(
            dag_id='test_sla_miss',
            sla_miss_callback=sla_callback,
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta(days=1)},
        )

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        # Create a TaskInstance for two days ago
        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(
            SlaMiss(
                task_id='dummy',
                dag_id='test_sla_miss',
                execution_date=test_start_date,
                email_sent=False,
                notification_sent=True,
            )
        )

        # Now call manage_slas and see if the sla_miss callback gets called
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_file_processor.manage_slas(dag=dag, session=session)

        sla_callback.assert_not_called()

    def test_dag_file_processor_sla_miss_callback_exception(self):
        """
        Test that the dag file processor gracefully logs an exception if there is a problem
        calling the sla_miss_callback
        """
        session = settings.Session()

        sla_callback = MagicMock(side_effect=RuntimeError('Could not call function'))

        test_start_date = days_ago(2)
        dag = DAG(
            dag_id='test_sla_miss',
            sla_miss_callback=sla_callback,
            default_args={'start_date': test_start_date},
        )

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow', sla=datetime.timedelta(hours=1))

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        # Now call manage_slas and see if the sla_miss callback gets called
        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock_log)
        dag_file_processor.manage_slas(dag=dag, session=session)
        assert sla_callback.called
        mock_log.exception.assert_called_once_with(
            'Could not call sla_miss_callback for DAG %s', 'test_sla_miss'
        )

    @mock.patch('airflow.dag_processing.processor.send_email')
    def test_dag_file_processor_only_collect_emails_from_sla_missed_tasks(self, mock_send_email):
        session = settings.Session()

        test_start_date = days_ago(2)
        dag = DAG(
            dag_id='test_sla_miss',
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta(days=1)},
        )

        email1 = 'test1@test.com'
        task = DummyOperator(
            task_id='sla_missed', dag=dag, owner='airflow', email=email1, sla=datetime.timedelta(hours=1)
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        email2 = 'test2@test.com'
        DummyOperator(task_id='sla_not_missed', dag=dag, owner='airflow', email=email2)

        session.merge(SlaMiss(task_id='sla_missed', dag_id='test_sla_miss', execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())

        dag_file_processor.manage_slas(dag=dag, session=session)

        assert len(mock_send_email.call_args_list) == 1

        send_email_to = mock_send_email.call_args_list[0][0][0]
        assert email1 in send_email_to
        assert email2 not in send_email_to

    @mock.patch('airflow.dag_processing.processor.Stats.incr')
    @mock.patch("airflow.utils.email.send_email")
    def test_dag_file_processor_sla_miss_email_exception(self, mock_send_email, mock_stats_incr):
        """
        Test that the dag file processor gracefully logs an exception if there is a problem
        sending an email
        """
        session = settings.Session()

        # Mock the callback function so we can verify that it was not called
        mock_send_email.side_effect = RuntimeError('Could not send an email')

        test_start_date = days_ago(2)
        dag = DAG(
            dag_id='test_sla_miss',
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta(days=1)},
        )

        task = DummyOperator(
            task_id='dummy', dag=dag, owner='airflow', email='test@test.com', sla=datetime.timedelta(hours=1)
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock_log)

        dag_file_processor.manage_slas(dag=dag, session=session)
        mock_log.exception.assert_called_once_with(
            'Could not send SLA Miss email notification for DAG %s', 'test_sla_miss'
        )
        mock_stats_incr.assert_called_once_with('sla_email_notification_failure')

    def test_dag_file_processor_sla_miss_deleted_task(self):
        """
        Test that the dag file processor will not crash when trying to send
        sla miss notification for a deleted task
        """
        session = settings.Session()

        test_start_date = days_ago(2)
        dag = DAG(
            dag_id='test_sla_miss',
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta(days=1)},
        )

        task = DummyOperator(
            task_id='dummy', dag=dag, owner='airflow', email='test@test.com', sla=datetime.timedelta(hours=1)
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(
            SlaMiss(task_id='dummy_deleted', dag_id='test_sla_miss', execution_date=test_start_date)
        )

        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock_log)
        dag_file_processor.manage_slas(dag=dag, session=session)

    @patch.object(TaskInstance, 'handle_failure_with_callback')
    def test_execute_on_failure_callbacks(self, mock_ti_handle_failure):
        dagbag = DagBag(dag_folder="/dev/null", include_examples=True, read_dags_from_db=False)
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        with create_session() as session:
            session.query(TaskInstance).delete()
            dag = dagbag.get_dag('example_branch_operator')
            task = dag.get_task(task_id='run_this_first')

            ti = TaskInstance(task, DEFAULT_DATE, State.RUNNING)

            session.add(ti)
            session.commit()

            requests = [
                TaskCallbackRequest(
                    full_filepath="A", simple_task_instance=SimpleTaskInstance(ti), msg="Message"
                )
            ]
            dag_file_processor.execute_callbacks(dagbag, requests)
            mock_ti_handle_failure.assert_called_once_with(
                error="Message",
                test_mode=conf.getboolean('core', 'unit_test_mode'),
            )

    def test_failure_callbacks_should_not_drop_hostname(self):
        dagbag = DagBag(dag_folder="/dev/null", include_examples=True, read_dags_from_db=False)
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_file_processor.UNIT_TEST_MODE = False

        with create_session() as session:
            dag = dagbag.get_dag('example_branch_operator')
            task = dag.get_task(task_id='run_this_first')

            ti = TaskInstance(task, DEFAULT_DATE, State.RUNNING)
            ti.hostname = "test_hostname"
            session.add(ti)

        with create_session() as session:
            requests = [
                TaskCallbackRequest(
                    full_filepath="A", simple_task_instance=SimpleTaskInstance(ti), msg="Message"
                )
            ]
            dag_file_processor.execute_callbacks(dagbag, requests)
            tis = session.query(TaskInstance)
            assert tis[0].hostname == "test_hostname"

    def test_process_file_should_failure_callback(self):
        dag_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), '../dags/test_on_failure_callback.py'
        )
        dagbag = DagBag(dag_folder=dag_file, include_examples=False)
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        with create_session() as session, NamedTemporaryFile(delete=False) as callback_file:
            session.query(TaskInstance).delete()
            dag = dagbag.get_dag('test_om_failure_callback_dag')
            task = dag.get_task(task_id='test_om_failure_callback_task')

            ti = TaskInstance(task, DEFAULT_DATE, State.RUNNING)

            session.add(ti)
            session.commit()

            requests = [
                TaskCallbackRequest(
                    full_filepath=dag.fileloc,
                    simple_task_instance=SimpleTaskInstance(ti),
                    msg="Message",
                )
            ]
            callback_file.close()

            with mock.patch.dict("os.environ", {"AIRFLOW_CALLBACK_FILE": callback_file.name}):
                dag_file_processor.process_file(dag_file, requests)
            with open(callback_file.name) as callback_file2:
                content = callback_file2.read()
            assert "Callback fired" == content
            os.remove(callback_file.name)

    @conf_vars({("core", "dagbag_import_error_tracebacks"): "False"})
    def test_add_unparseable_file_before_sched_start_creates_import_error(self, tmpdir):
        unparseable_filename = tmpdir / TEMP_DAG_FILENAME
        with open(unparseable_filename, 'w') as unparseable_file:
            unparseable_file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(unparseable_filename, session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 1
            import_error = import_errors[0]
            assert import_error.filename == unparseable_filename
            assert import_error.stacktrace == f"invalid syntax ({TEMP_DAG_FILENAME}, line 1)"
            session.rollback()

    def test_no_import_errors_with_parseable_dag(self, tmpdir):
        parseable_filename = tmpdir / TEMP_DAG_FILENAME

        with open(parseable_filename, 'w') as parseable_file:
            parseable_file.writelines(PARSEABLE_DAG_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(parseable_file, session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 0

            session.rollback()

    @conf_vars({("core", "dagbag_import_error_tracebacks"): "False"})
    def test_new_import_error_replaces_old(self, tmpdir):
        unparseable_filename = tmpdir / TEMP_DAG_FILENAME

        # Generate original import error
        with open(unparseable_filename, 'w') as unparseable_file:
            unparseable_file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
        session = settings.Session()
        self._process_file(unparseable_filename, session)

        # Generate replacement import error (the error will be on the second line now)
        with open(unparseable_filename, 'w') as unparseable_file:
            unparseable_file.writelines(
                PARSEABLE_DAG_FILE_CONTENTS + os.linesep + UNPARSEABLE_DAG_FILE_CONTENTS
            )
        self._process_file(unparseable_filename, session)

        import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == unparseable_filename
        assert import_error.stacktrace == f"invalid syntax ({TEMP_DAG_FILENAME}, line 2)"

        session.rollback()

    def test_remove_error_clears_import_error(self, tmpdir):
        filename_to_parse = tmpdir / TEMP_DAG_FILENAME

        # Generate original import error
        with open(filename_to_parse, 'w') as file_to_parse:
            file_to_parse.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
        session = settings.Session()
        self._process_file(filename_to_parse, session)

        # Remove the import error from the file
        with open(filename_to_parse, 'w') as file_to_parse:
            file_to_parse.writelines(PARSEABLE_DAG_FILE_CONTENTS)
        self._process_file(filename_to_parse, session)

        import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 0

        session.rollback()

    def test_import_error_tracebacks(self, tmpdir):
        unparseable_filename = tmpdir / TEMP_DAG_FILENAME
        with open(unparseable_filename, "w") as unparseable_file:
            unparseable_file.writelines(INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(unparseable_filename, session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 1
            import_error = import_errors[0]
            assert import_error.filename == unparseable_filename
            expected_stacktrace = (
                "Traceback (most recent call last):\n"
                '  File "{}", line 3, in <module>\n'
                "    something()\n"
                '  File "{}", line 2, in something\n'
                "    return airflow_DAG\n"
                "NameError: name 'airflow_DAG' is not defined\n"
            )
            assert import_error.stacktrace == expected_stacktrace.format(
                unparseable_filename, unparseable_filename
            )
            session.rollback()

    @conf_vars({("core", "dagbag_import_error_traceback_depth"): "1"})
    def test_import_error_traceback_depth(self, tmpdir):
        unparseable_filename = tmpdir / TEMP_DAG_FILENAME
        with open(unparseable_filename, "w") as unparseable_file:
            unparseable_file.writelines(INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(unparseable_filename, session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 1
            import_error = import_errors[0]
            assert import_error.filename == unparseable_filename
            expected_stacktrace = (
                "Traceback (most recent call last):\n"
                '  File "{}", line 2, in something\n'
                "    return airflow_DAG\n"
                "NameError: name 'airflow_DAG' is not defined\n"
            )
            assert import_error.stacktrace == expected_stacktrace.format(unparseable_filename)

            session.rollback()

    def test_import_error_tracebacks_zip(self, tmpdir):
        invalid_zip_filename = tmpdir / "test_zip_invalid.zip"
        invalid_dag_filename = invalid_zip_filename / TEMP_DAG_FILENAME
        with ZipFile(invalid_zip_filename, "w") as invalid_zip_file:
            invalid_zip_file.writestr(TEMP_DAG_FILENAME, INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(invalid_zip_filename, session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 1
            import_error = import_errors[0]
            assert import_error.filename == invalid_zip_filename
            expected_stacktrace = (
                "Traceback (most recent call last):\n"
                '  File "{}", line 3, in <module>\n'
                "    something()\n"
                '  File "{}", line 2, in something\n'
                "    return airflow_DAG\n"
                "NameError: name 'airflow_DAG' is not defined\n"
            )
            assert import_error.stacktrace == expected_stacktrace.format(
                invalid_dag_filename, invalid_dag_filename
            )
            session.rollback()

    @conf_vars({("core", "dagbag_import_error_traceback_depth"): "1"})
    def test_import_error_tracebacks_zip_depth(self, tmpdir):
        invalid_zip_filename = tmpdir / "test_zip_invalid.zip"
        invalid_dag_filename = invalid_zip_filename / TEMP_DAG_FILENAME
        with ZipFile(invalid_zip_filename, "w") as invalid_zip_file:
            invalid_zip_file.writestr(TEMP_DAG_FILENAME, INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)

        with create_session() as session:
            self._process_file(invalid_zip_filename, session)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 1
            import_error = import_errors[0]
            assert import_error.filename == invalid_zip_filename
            expected_stacktrace = (
                "Traceback (most recent call last):\n"
                '  File "{}", line 2, in something\n'
                "    return airflow_DAG\n"
                "NameError: name 'airflow_DAG' is not defined\n"
            )
            assert import_error.stacktrace == expected_stacktrace.format(invalid_dag_filename)
            session.rollback()
