# -*- coding: utf-8 -*-
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
import subprocess
import contextlib
import os
import sys
import unittest
from multiprocessing import Pool

# leave this it is used by the test worker
# noinspection PyUnresolvedReferences
import celery.contrib.testing.tasks  # noqa: F401 pylint: disable=unused-import
import mock
import pytest
from celery import Celery
from celery import states as celery_states
from celery.contrib.testing.worker import start_worker
from kombu.asynchronous import set_event_loop
from parameterized import parameterized

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors import celery_executor
from airflow.utils.state import State


def _prepare_test_bodies():
    if 'CELERY_BROKER_URLS' in os.environ:
        return [
            (url, )
            for url in os.environ['CELERY_BROKER_URLS'].split(',')
        ]
    return [(conf.get('celery', 'BROKER_URL'))]


class TestCeleryExecutor(unittest.TestCase):

    @contextlib.contextmanager
    def _prepare_app(self, broker_url=None, execute=None):
        broker_url = broker_url or conf.get('celery', 'BROKER_URL')
        execute = execute or celery_executor.execute_command.__wrapped__

        test_config = dict(celery_executor.celery_configuration)
        test_config.update({'broker_url': broker_url})
        test_app = Celery(broker_url, config_source=test_config)
        test_execute = test_app.task(execute)
        patch_app = mock.patch('airflow.executors.celery_executor.app', test_app)
        patch_execute = mock.patch('airflow.executors.celery_executor.execute_command', test_execute)

        backend = test_app.backend

        if hasattr(backend, 'ResultSession'):
            # Pre-create the database tables now, otherwise SQLA vis Celery has a
            # race condition where it one of the subprocesses can die with "Table
            # already exists" error, because SQLA checks for which tables exist,
            # then issues a CREATE TABLE, rather than doing CREATE TABLE IF NOT
            # EXISTS
            session = backend.ResultSession()
            session.close()

        with patch_app, patch_execute:
            try:
                yield test_app
            finally:
                # Clear event loop to tear down each celery instance
                set_event_loop(None)

    @parameterized.expand(_prepare_test_bodies())
    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_celery_integration(self, broker_url):
        success_command = ['airflow', 'run', 'true', 'some_parameter']
        fail_command = ['airflow', 'version']

        def fake_execute_command(command):
            if command != success_command:
                raise AirflowException("fail")

        with self._prepare_app(broker_url, execute=fake_execute_command) as app:
            executor = celery_executor.CeleryExecutor()
            executor.start()

            with start_worker(app=app, logfile=sys.stdout, loglevel='debug'):
                cached_celery_backend = celery_executor.execute_command.backend
                task_tuples_to_send = [('success', 'fake_simple_ti', success_command,
                                        celery_executor.celery_configuration['task_default_queue'],
                                        celery_executor.execute_command),
                                       ('fail', 'fake_simple_ti', fail_command,
                                        celery_executor.celery_configuration['task_default_queue'],
                                        celery_executor.execute_command)]

                chunksize = executor._num_tasks_per_send_process(len(task_tuples_to_send))
                num_processes = min(len(task_tuples_to_send), executor._sync_parallelism)

                send_pool = Pool(processes=num_processes)
                key_and_async_results = send_pool.map(
                    celery_executor.send_task_to_executor,
                    task_tuples_to_send,
                    chunksize=chunksize)

                send_pool.close()
                send_pool.join()

                for key, command, result in key_and_async_results:
                    # Only pops when enqueued successfully, otherwise keep it
                    # and expect scheduler loop to deal with it.
                    result.backend = cached_celery_backend
                    executor.running[key] = command
                    executor.tasks[key] = result
                    executor.last_state[key] = celery_states.PENDING

                executor.running['success'] = True
                executor.running['fail'] = True

                executor.end(synchronous=True)

        self.assertTrue(executor.event_buffer['success'], State.SUCCESS)
        self.assertTrue(executor.event_buffer['fail'], State.FAILED)

        self.assertNotIn('success', executor.tasks)
        self.assertNotIn('fail', executor.tasks)

        self.assertNotIn('success', executor.last_state)
        self.assertNotIn('fail', executor.last_state)

    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_error_sending_task(self):
        def fake_execute_command():
            pass

        with self._prepare_app(execute=fake_execute_command):
            # fake_execute_command takes no arguments while execute_command takes 1,
            # which will cause TypeError when calling task.apply_async()
            executor = celery_executor.CeleryExecutor()
            value_tuple = 'command', '_', 'queue', 'should_be_a_simple_ti'
            executor.queued_tasks['key'] = value_tuple
            executor.heartbeat()
        self.assertEquals(1, len(executor.queued_tasks))
        self.assertEquals(executor.queued_tasks['key'], value_tuple)

    @pytest.mark.backend("mysql", "postgres")
    def test_exception_propagation(self):
        with self._prepare_app() as app:
            @app.task
            def fake_celery_task():
                return {}

            mock_log = mock.MagicMock()
            executor = celery_executor.CeleryExecutor()
            executor._log = mock_log

            executor.tasks = {'key': fake_celery_task()}
            executor.sync()

        assert mock_log.error.call_count == 1
        args, kwargs = mock_log.error.call_args_list[0]
        # Result of queuing is not a celery task but a dict,
        # and it should raise AttributeError and then get propagated
        # to the error log.
        self.assertIn(celery_executor.CELERY_FETCH_ERR_MSG_HEADER, args[0])
        self.assertIn('AttributeError', args[1])

    @mock.patch('airflow.executors.celery_executor.CeleryExecutor.sync')
    @mock.patch('airflow.executors.celery_executor.CeleryExecutor.trigger_tasks')
    @mock.patch('airflow.settings.Stats.gauge')
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync):
        executor = celery_executor.CeleryExecutor()
        executor.heartbeat()
        calls = [mock.call('executor.open_slots', mock.ANY),
                 mock.call('executor.queued_tasks', mock.ANY),
                 mock.call('executor.running_tasks', mock.ANY)]
        mock_stats_gauge.assert_has_calls(calls)

    @parameterized.expand((
        [['true'], ValueError],
        [['airflow', 'version'], ValueError],
        [['airflow', 'run'], None]
    ))
    @mock.patch('subprocess.check_call')
    def test_command_validation(self, command, expected_exception, mock_check_call):
        # Check that we validate _on the receiving_ side, not just sending side
        if expected_exception:
            with pytest.raises(expected_exception):
                celery_executor.execute_command(command)
            mock_check_call.assert_not_called()
        else:
            celery_executor.execute_command(command)
            mock_check_call.assert_called_once_with(
                command, stderr=mock.ANY, close_fds=mock.ANY, env=mock.ANY,
            )

    @mock.patch('subprocess.check_call')
    def test_execute_command_success(self, mock_check_call):
        fake_command = ['airflow', 'run']
        celery_executor.execute_command(fake_command, num_attempts=3)
        # Subprocess call should only happen once if successful the first time.
        mock_check_call.assert_called_once_with(
            fake_command, stderr=subprocess.STDOUT, close_fds=True,
            env=mock.ANY)

    @mock.patch('subprocess.check_call')
    @mock.patch('time.sleep')
    def test_execute_command_eventual_success(self, mock_sleep, mock_check_call):
        fake_command = ['airflow', 'run']
        fake_return_code = 1
        mock_check_call.side_effect = [
            subprocess.CalledProcessError(fake_return_code, fake_command),
            None,
        ]
        celery_executor.execute_command(fake_command, num_attempts=3)
        call = mock.call(
            fake_command, stderr=subprocess.STDOUT, close_fds=True,
            env=mock.ANY)
        mock_check_call.assert_has_calls([call, call])
        self.assertEqual(mock_sleep.call_count, 1)

    @mock.patch('subprocess.check_call')
    def test_execute_command_fail(self, mock_check_call):
        fake_command = ['airflow', 'run']
        fake_return_code = 1
        mock_check_call.side_effect = subprocess.CalledProcessError(
            fake_return_code, fake_command)

        with self.assertRaises(AirflowException):
            celery_executor.execute_command(fake_command, num_attempts=1)
        mock_check_call.assert_called_once_with(
            fake_command, stderr=subprocess.STDOUT, close_fds=True,
            env=mock.ANY)

    @mock.patch('subprocess.check_call')
    @mock.patch('time.sleep')
    def test_execute_command_fail_with_retries(self, mock_sleep, mock_check_call):
        fake_command = ['airflow', 'run']
        fake_return_code = 1
        mock_check_call.side_effect = subprocess.CalledProcessError(
            fake_return_code, fake_command)

        with self.assertRaises(AirflowException):
            celery_executor.execute_command(fake_command, num_attempts=3)
        call = mock.call(
            fake_command, stderr=subprocess.STDOUT, close_fds=True,
            env=mock.ANY)
        mock_check_call.assert_has_calls([call, call, call])
        # The last attempt should not wait after failing.
        self.assertEqual(mock_sleep.call_count, 2)


def test_operation_timeout_config():
    assert celery_executor.OPERATION_TIMEOUT == 2


if __name__ == '__main__':
    unittest.main()
