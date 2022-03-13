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
import unittest
from datetime import datetime
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.providers.databricks.hooks.databricks import RunState
from airflow.providers.databricks.operators import databricks as databricks_operator
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
)

DATE = '2017-04-20'
TASK_ID = 'databricks-operator'
DEFAULT_CONN_ID = 'databricks_default'
NOTEBOOK_TASK = {'notebook_path': '/test'}
TEMPLATED_NOTEBOOK_TASK = {'notebook_path': '/test-{{ ds }}'}
RENDERED_TEMPLATED_NOTEBOOK_TASK = {'notebook_path': f'/test-{DATE}'}
SPARK_JAR_TASK = {'main_class_name': 'com.databricks.Test'}
SPARK_PYTHON_TASK = {'python_file': 'test.py', 'parameters': ['--param', '123']}
SPARK_SUBMIT_TASK = {
    "parameters": ["--class", "org.apache.spark.examples.SparkPi", "dbfs:/path/to/examples.jar", "10"]
}
NEW_CLUSTER = {'spark_version': '2.0.x-scala2.10', 'node_type_id': 'development-node', 'num_workers': 1}
EXISTING_CLUSTER_ID = 'existing-cluster-id'
RUN_NAME = 'run-name'
RUN_ID = 1
JOB_ID = "42"
JOB_NAME = "job-name"
NOTEBOOK_PARAMS = {"dry-run": "true", "oldest-time-to-consider": "1457570074236"}
JAR_PARAMS = ["param1", "param2"]
RENDERED_TEMPLATED_JAR_PARAMS = [f'/test-{DATE}']
TEMPLATED_JAR_PARAMS = ['/test-{{ ds }}']
PYTHON_PARAMS = ["john doe", "35"]
SPARK_SUBMIT_PARAMS = ["--class", "org.apache.spark.examples.SparkPi"]


class TestDatabricksOperatorSharedFunctions(unittest.TestCase):
    def test_deep_string_coerce(self):
        test_json = {
            'test_int': 1,
            'test_float': 1.0,
            'test_dict': {'key': 'value'},
            'test_list': [1, 1.0, 'a', 'b'],
            'test_tuple': (1, 1.0, 'a', 'b'),
        }

        expected = {
            'test_int': '1',
            'test_float': '1.0',
            'test_dict': {'key': 'value'},
            'test_list': ['1', '1.0', 'a', 'b'],
            'test_tuple': ['1', '1.0', 'a', 'b'],
        }
        assert databricks_operator._deep_string_coerce(test_json) == expected


class TestDatabricksSubmitRunOperator(unittest.TestCase):
    def test_init_with_notebook_task_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID, new_cluster=NEW_CLUSTER, notebook_task=NOTEBOOK_TASK
        )
        expected = databricks_operator._deep_string_coerce(
            {'new_cluster': NEW_CLUSTER, 'notebook_task': NOTEBOOK_TASK, 'run_name': TASK_ID}
        )

        assert expected == op.json

    def test_init_with_spark_python_task_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID, new_cluster=NEW_CLUSTER, spark_python_task=SPARK_PYTHON_TASK
        )
        expected = databricks_operator._deep_string_coerce(
            {'new_cluster': NEW_CLUSTER, 'spark_python_task': SPARK_PYTHON_TASK, 'run_name': TASK_ID}
        )

        assert expected == op.json

    def test_init_with_spark_submit_task_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksSubmitRunOperator(
            task_id=TASK_ID, new_cluster=NEW_CLUSTER, spark_submit_task=SPARK_SUBMIT_TASK
        )
        expected = databricks_operator._deep_string_coerce(
            {'new_cluster': NEW_CLUSTER, 'spark_submit_task': SPARK_SUBMIT_TASK, 'run_name': TASK_ID}
        )

        assert expected == op.json

    def test_init_with_json(self):
        """
        Test the initializer with json data.
        """
        json = {'new_cluster': NEW_CLUSTER, 'notebook_task': NOTEBOOK_TASK}
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        expected = databricks_operator._deep_string_coerce(
            {'new_cluster': NEW_CLUSTER, 'notebook_task': NOTEBOOK_TASK, 'run_name': TASK_ID}
        )
        assert expected == op.json

    def test_init_with_tasks(self):
        tasks = [{"task_key": 1, "new_cluster": NEW_CLUSTER, "notebook_task": NOTEBOOK_TASK}]
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, tasks=tasks)
        expected = databricks_operator._deep_string_coerce({'run_name': TASK_ID, "tasks": tasks})
        assert expected == op.json

    def test_init_with_specified_run_name(self):
        """
        Test the initializer with a specified run_name.
        """
        json = {'new_cluster': NEW_CLUSTER, 'notebook_task': NOTEBOOK_TASK, 'run_name': RUN_NAME}
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        expected = databricks_operator._deep_string_coerce(
            {'new_cluster': NEW_CLUSTER, 'notebook_task': NOTEBOOK_TASK, 'run_name': RUN_NAME}
        )
        assert expected == op.json

    def test_pipeline_task(self):
        """
        Test the initializer with a specified run_name.
        """
        pipeline_task = {"pipeline_id": "test-dlt"}
        json = {'new_cluster': NEW_CLUSTER, 'run_name': RUN_NAME, "pipeline_task": pipeline_task}
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)
        expected = databricks_operator._deep_string_coerce(
            {'new_cluster': NEW_CLUSTER, "pipeline_task": pipeline_task, 'run_name': RUN_NAME}
        )
        assert expected == op.json

    def test_init_with_merging(self):
        """
        Test the initializer when json and other named parameters are both
        provided. The named parameters should override top level keys in the
        json dict.
        """
        override_new_cluster = {'workers': 999}
        json = {
            'new_cluster': NEW_CLUSTER,
            'notebook_task': NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=json, new_cluster=override_new_cluster)
        expected = databricks_operator._deep_string_coerce(
            {
                'new_cluster': override_new_cluster,
                'notebook_task': NOTEBOOK_TASK,
                'run_name': TASK_ID,
            }
        )
        assert expected == op.json

    def test_init_with_templating(self):
        json = {
            'new_cluster': NEW_CLUSTER,
            'notebook_task': TEMPLATED_NOTEBOOK_TASK,
        }
        dag = DAG('test', start_date=datetime.now())
        op = DatabricksSubmitRunOperator(dag=dag, task_id=TASK_ID, json=json)
        op.render_template_fields(context={'ds': DATE})
        expected = databricks_operator._deep_string_coerce(
            {
                'new_cluster': NEW_CLUSTER,
                'notebook_task': RENDERED_TEMPLATED_NOTEBOOK_TASK,
                'run_name': TASK_ID,
            }
        )
        assert expected == op.json

    def test_init_with_bad_type(self):
        json = {'test': datetime.now()}
        # Looks a bit weird since we have to escape regex reserved symbols.
        exception_message = (
            r'Type \<(type|class) \'datetime.datetime\'\> used '
            + r'for parameter json\[test\] is not a number or a string'
        )
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksSubmitRunOperator(task_id=TASK_ID, json=json)

    @mock.patch('airflow.providers.databricks.operators.databricks.DatabricksHook')
    def test_exec_success(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        run = {
            'new_cluster': NEW_CLUSTER,
            'notebook_task': NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'SUCCESS', '')

        op.execute(None)

        expected = databricks_operator._deep_string_coerce(
            {'new_cluster': NEW_CLUSTER, 'notebook_task': NOTEBOOK_TASK, 'run_name': TASK_ID}
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
        )

        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
        assert RUN_ID == op.run_id

    @mock.patch('airflow.providers.databricks.operators.databricks.DatabricksHook')
    def test_exec_failure(self, db_mock_class):
        """
        Test the execute function in case where the run failed.
        """
        run = {
            'new_cluster': NEW_CLUSTER,
            'notebook_task': NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'FAILED', '')

        with pytest.raises(AirflowException):
            op.execute(None)

        expected = databricks_operator._deep_string_coerce(
            {
                'new_cluster': NEW_CLUSTER,
                'notebook_task': NOTEBOOK_TASK,
                'run_name': TASK_ID,
            }
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
        )
        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
        assert RUN_ID == op.run_id

    @mock.patch('airflow.providers.databricks.operators.databricks.DatabricksHook')
    def test_on_kill(self, db_mock_class):
        run = {
            'new_cluster': NEW_CLUSTER,
            'notebook_task': NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        op.run_id = RUN_ID

        op.on_kill()

        db_mock.cancel_run.assert_called_once_with(RUN_ID)

    @mock.patch('airflow.providers.databricks.operators.databricks.DatabricksHook')
    def test_wait_for_termination(self, db_mock_class):
        run = {
            'new_cluster': NEW_CLUSTER,
            'notebook_task': NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'SUCCESS', '')

        assert op.wait_for_termination

        op.execute(None)

        expected = databricks_operator._deep_string_coerce(
            {'new_cluster': NEW_CLUSTER, 'notebook_task': NOTEBOOK_TASK, 'run_name': TASK_ID}
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
        )

        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)

    @mock.patch('airflow.providers.databricks.operators.databricks.DatabricksHook')
    def test_no_wait_for_termination(self, db_mock_class):
        run = {
            'new_cluster': NEW_CLUSTER,
            'notebook_task': NOTEBOOK_TASK,
        }
        op = DatabricksSubmitRunOperator(task_id=TASK_ID, wait_for_termination=False, json=run)
        db_mock = db_mock_class.return_value
        db_mock.submit_run.return_value = 1

        assert not op.wait_for_termination

        op.execute(None)

        expected = databricks_operator._deep_string_coerce(
            {'new_cluster': NEW_CLUSTER, 'notebook_task': NOTEBOOK_TASK, 'run_name': TASK_ID}
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
        )

        db_mock.submit_run.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_not_called()


class TestDatabricksRunNowOperator(unittest.TestCase):
    def test_init_with_named_parameters(self):
        """
        Test the initializer with the named parameters.
        """
        op = DatabricksRunNowOperator(job_id=JOB_ID, task_id=TASK_ID)
        expected = databricks_operator._deep_string_coerce({'job_id': 42})

        assert expected == op.json

    def test_init_with_json(self):
        """
        Test the initializer with json data.
        """
        json = {
            'notebook_params': NOTEBOOK_PARAMS,
            'jar_params': JAR_PARAMS,
            'python_params': PYTHON_PARAMS,
            'spark_submit_params': SPARK_SUBMIT_PARAMS,
            'job_id': JOB_ID,
        }
        op = DatabricksRunNowOperator(task_id=TASK_ID, json=json)

        expected = databricks_operator._deep_string_coerce(
            {
                'notebook_params': NOTEBOOK_PARAMS,
                'jar_params': JAR_PARAMS,
                'python_params': PYTHON_PARAMS,
                'spark_submit_params': SPARK_SUBMIT_PARAMS,
                'job_id': JOB_ID,
            }
        )

        assert expected == op.json

    def test_init_with_merging(self):
        """
        Test the initializer when json and other named parameters are both
        provided. The named parameters should override top level keys in the
        json dict.
        """
        override_notebook_params = {'workers': "999"}
        override_jar_params = ['workers', "998"]
        json = {'notebook_params': NOTEBOOK_PARAMS, 'jar_params': JAR_PARAMS}

        op = DatabricksRunNowOperator(
            task_id=TASK_ID,
            json=json,
            job_id=JOB_ID,
            notebook_params=override_notebook_params,
            python_params=PYTHON_PARAMS,
            jar_params=override_jar_params,
            spark_submit_params=SPARK_SUBMIT_PARAMS,
        )

        expected = databricks_operator._deep_string_coerce(
            {
                'notebook_params': override_notebook_params,
                'jar_params': override_jar_params,
                'python_params': PYTHON_PARAMS,
                'spark_submit_params': SPARK_SUBMIT_PARAMS,
                'job_id': JOB_ID,
            }
        )

        assert expected == op.json

    def test_init_with_templating(self):
        json = {'notebook_params': NOTEBOOK_PARAMS, 'jar_params': TEMPLATED_JAR_PARAMS}

        dag = DAG('test', start_date=datetime.now())
        op = DatabricksRunNowOperator(dag=dag, task_id=TASK_ID, job_id=JOB_ID, json=json)
        op.render_template_fields(context={'ds': DATE})
        expected = databricks_operator._deep_string_coerce(
            {
                'notebook_params': NOTEBOOK_PARAMS,
                'jar_params': RENDERED_TEMPLATED_JAR_PARAMS,
                'job_id': JOB_ID,
            }
        )
        assert expected == op.json

    def test_init_with_bad_type(self):
        json = {'test': datetime.now()}
        # Looks a bit weird since we have to escape regex reserved symbols.
        exception_message = (
            r'Type \<(type|class) \'datetime.datetime\'\> used '
            + r'for parameter json\[test\] is not a number or a string'
        )
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=json)

    @mock.patch('airflow.providers.databricks.operators.databricks.DatabricksHook')
    def test_exec_success(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        run = {'notebook_params': NOTEBOOK_PARAMS, 'notebook_task': NOTEBOOK_TASK, 'jar_params': JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'SUCCESS', '')

        op.execute(None)

        expected = databricks_operator._deep_string_coerce(
            {
                'notebook_params': NOTEBOOK_PARAMS,
                'notebook_task': NOTEBOOK_TASK,
                'jar_params': JAR_PARAMS,
                'job_id': JOB_ID,
            }
        )

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
        )
        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
        assert RUN_ID == op.run_id

    @mock.patch('airflow.providers.databricks.operators.databricks.DatabricksHook')
    def test_exec_failure(self, db_mock_class):
        """
        Test the execute function in case where the run failed.
        """
        run = {'notebook_params': NOTEBOOK_PARAMS, 'notebook_task': NOTEBOOK_TASK, 'jar_params': JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'FAILED', '')

        with pytest.raises(AirflowException):
            op.execute(None)

        expected = databricks_operator._deep_string_coerce(
            {
                'notebook_params': NOTEBOOK_PARAMS,
                'notebook_task': NOTEBOOK_TASK,
                'jar_params': JAR_PARAMS,
                'job_id': JOB_ID,
            }
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
        )
        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
        assert RUN_ID == op.run_id

    @mock.patch('airflow.providers.databricks.operators.databricks.DatabricksHook')
    def test_on_kill(self, db_mock_class):
        run = {'notebook_params': NOTEBOOK_PARAMS, 'notebook_task': NOTEBOOK_TASK, 'jar_params': JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        op.run_id = RUN_ID

        op.on_kill()
        db_mock.cancel_run.assert_called_once_with(RUN_ID)

    @mock.patch('airflow.providers.databricks.operators.databricks.DatabricksHook')
    def test_wait_for_termination(self, db_mock_class):
        run = {'notebook_params': NOTEBOOK_PARAMS, 'notebook_task': NOTEBOOK_TASK, 'jar_params': JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'SUCCESS', '')

        assert op.wait_for_termination

        op.execute(None)

        expected = databricks_operator._deep_string_coerce(
            {
                'notebook_params': NOTEBOOK_PARAMS,
                'notebook_task': NOTEBOOK_TASK,
                'jar_params': JAR_PARAMS,
                'job_id': JOB_ID,
            }
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
        )

        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)

    @mock.patch('airflow.providers.databricks.operators.databricks.DatabricksHook')
    def test_no_wait_for_termination(self, db_mock_class):
        run = {'notebook_params': NOTEBOOK_PARAMS, 'notebook_task': NOTEBOOK_TASK, 'jar_params': JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, wait_for_termination=False, json=run)
        db_mock = db_mock_class.return_value
        db_mock.run_now.return_value = 1

        assert not op.wait_for_termination

        op.execute(None)

        expected = databricks_operator._deep_string_coerce(
            {
                'notebook_params': NOTEBOOK_PARAMS,
                'notebook_task': NOTEBOOK_TASK,
                'jar_params': JAR_PARAMS,
                'job_id': JOB_ID,
            }
        )
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
        )

        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_not_called()

    def test_init_exeption_with_job_name_and_job_id(self):
        exception_message = "Argument 'job_name' is not allowed with argument 'job_id'"

        with pytest.raises(AirflowException, match=exception_message):
            DatabricksRunNowOperator(task_id=TASK_ID, job_id=JOB_ID, job_name=JOB_NAME)

        with pytest.raises(AirflowException, match=exception_message):
            run = {'job_id': JOB_ID, 'job_name': JOB_NAME}
            DatabricksRunNowOperator(task_id=TASK_ID, json=run)

        with pytest.raises(AirflowException, match=exception_message):
            run = {'job_id': JOB_ID}
            DatabricksRunNowOperator(task_id=TASK_ID, json=run, job_name=JOB_NAME)

    @mock.patch('airflow.providers.databricks.operators.databricks.DatabricksHook')
    def test_exec_with_job_name(self, db_mock_class):
        run = {'notebook_params': NOTEBOOK_PARAMS, 'notebook_task': NOTEBOOK_TASK, 'jar_params': JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_name=JOB_NAME, json=run)
        db_mock = db_mock_class.return_value
        db_mock.find_job_id_by_name.return_value = JOB_ID
        db_mock.run_now.return_value = 1
        db_mock.get_run_state.return_value = RunState('TERMINATED', 'SUCCESS', '')

        op.execute(None)

        expected = databricks_operator._deep_string_coerce(
            {
                'notebook_params': NOTEBOOK_PARAMS,
                'notebook_task': NOTEBOOK_TASK,
                'jar_params': JAR_PARAMS,
                'job_id': JOB_ID,
            }
        )

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            retry_args=None,
        )
        db_mock.find_job_id_by_name.assert_called_once_with(JOB_NAME)
        db_mock.run_now.assert_called_once_with(expected)
        db_mock.get_run_page_url.assert_called_once_with(RUN_ID)
        db_mock.get_run_state.assert_called_once_with(RUN_ID)
        assert RUN_ID == op.run_id

    @mock.patch('airflow.providers.databricks.operators.databricks.DatabricksHook')
    def test_exec_failure_if_job_id_not_found(self, db_mock_class):
        run = {'notebook_params': NOTEBOOK_PARAMS, 'notebook_task': NOTEBOOK_TASK, 'jar_params': JAR_PARAMS}
        op = DatabricksRunNowOperator(task_id=TASK_ID, job_name=JOB_NAME, json=run)
        db_mock = db_mock_class.return_value
        db_mock.find_job_id_by_name.return_value = None

        exception_message = f"Job ID for job name {JOB_NAME} can not be found"
        with pytest.raises(AirflowException, match=exception_message):
            op.execute(None)

        db_mock.find_job_id_by_name.assert_called_once_with(JOB_NAME)
