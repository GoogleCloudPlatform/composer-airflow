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

"""Unit tests for stringified DAGs."""

import json
import mock
import multiprocessing
import unittest
from datetime import datetime

from airflow import example_dags
from airflow.contrib import example_dags as contrib_example_dags
from airflow.dag.serialization import Serialization, SerializedBaseOperator, SerializedDAG
from airflow.dag.serialization.enums import Encoding
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator, Connection, DAG, DagBag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator

serialized_simple_dag_ground_truth = {
    u"__type": u"dag",
    u"__var": {
        u"default_args": {u"__var": {}, u"__type": u"dict"},
        u"params": {u"__var": {}, u"__type": u"dict"},
        u"_dag_id": u"simple_dag",
        u"_default_view": u"tree",
        u"_concurrency": 16,
        u"_description": u"",
        u"fileloc": None,
        u"task_dict": {
            u"__var": {
                u"simple_task": {
                    u"__var": {
                        u"task_id": u"simple_task",
                        u"owner": u"airflow",
                        u"email_on_retry": True,
                        u"email_on_failure": True,
                        u"start_date": {u"__var": 1564617600.0,
                                        u"__type": u"datetime"},
                        u"trigger_rule": u"all_success",
                        u"depends_on_past": False,
                        u"wait_for_downstream": False,
                        u"retries": 0,
                        u"queue": u"default",
                        u"retry_delay": {u"__var": 300.0, u"__type": u"timedelta"},
                        u"retry_exponential_backoff": False,
                        u"params": {u"__var": {}, u"__type": u"dict"},
                        u"priority_weight": 1,
                        u"weight_rule": u"downstream",
                        u"executor_config": {u"__var": {}, u"__type": u"dict"},
                        u"_upstream_task_ids": {u"__var": [], u"__type": u"set"},
                        u"_downstream_task_ids": {u"__var": [], u"__type": u"set"},
                        u"_inlets": {
                            u"__var": {u"auto": False, u"task_ids": [], u"datasets": []},
                            u"__type": u"dict"},
                        u"_outlets": {u"__var": {u"datasets": []}, u"__type": u"dict"},
                        u"ui_color": u"#fff",
                        u"ui_fgcolor": u"#000",
                        u"template_fields": [],
                        u"_task_type": u"BaseOperator",
                        u"resources": None},
                    u"__type": u"operator"}},
            u"__type": u"dict"},
        u"timezone": {u"__var": u"UTC", u"__type": u"timezone"},
        u"schedule_interval": {u"__var": 86400.0, u"__type": u"timedelta"},
        u"max_active_runs": 16,
        u"orientation": u"LR",
        u"catchup": True,
        u"is_subdag": False,
    }}


def make_example_dags(module):
    """Loads DAGs from a module for test."""
    dagbag = DagBag(module.__path__[0])
    return dagbag.dags


def make_simple_dag():
    """Make very simple DAG to verify serialization result."""
    dag = DAG(dag_id='simple_dag')
    _ = BaseOperator(task_id='simple_task', dag=dag,
                     start_date=datetime(2019, 8, 1), owner="airflow")
    return {'simple_dag': dag}


def make_user_defined_macro_filter_dag():
    """ Make DAGs with user defined macros and filters using locally defined methods.

    For Webserver, we do not include ``user_defined_macros`` & ``user_defined_filters``.

    The examples here test:
        (1) functions can be successfully displayed on UI;
        (2) templates with function macros have been rendered before serialization.
    """

    def compute_next_execution_date(dag, execution_date):
        return dag.following_schedule(execution_date)

    default_args = {
        'start_date': datetime(2019, 7, 10)
    }
    dag = DAG(
        'user_defined_macro_filter_dag',
        default_args=default_args,
        user_defined_macros={
            'next_execution_date': compute_next_execution_date,
        },
        user_defined_filters={
            'hello': lambda name: 'Hello %s' % name
        },
        catchup=False
    )
    _ = BashOperator(
        task_id='echo',
        bash_command='echo "{{ next_execution_date(dag, execution_date) }}"',
        dag=dag,
    )
    return {dag.dag_id: dag}


def collect_dags():
    """Collects DAGs to test."""
    dags = {}
    dags.update(make_simple_dag())
    dags.update(make_user_defined_macro_filter_dag())
    dags.update(make_example_dags(example_dags))
    dags.update(make_example_dags(contrib_example_dags))
    return dags


def serialize_subprocess(queue):
    """Validate pickle in a subprocess."""
    dags = collect_dags()
    for dag in dags.values():
        queue.put(Serialization.to_json(dag))
    queue.put(None)


class TestStringifiedDAGs(unittest.TestCase):
    """Unit tests for stringified DAGs."""

    maxDiff = None

    def setUp(self):
        super(TestStringifiedDAGs, self).setUp()
        BaseHook.get_connection = mock.Mock(
            return_value=Connection(
                extra=('{'
                       '"project_id": "mock", '
                       '"location": "mock", '
                       '"instance": "mock", '
                       '"database_type": "postgres", '
                       '"use_proxy": "False", '
                       '"use_ssl": "False"'
                       '}')))

    def test_serialization(self):
        """Serialization and deserialization should work for every DAG and Operator."""
        dags = collect_dags()
        serialized_dags = {}
        for _, v in dags.items():
            dag = Serialization.to_json(v)
            serialized_dags[v.dag_id] = dag

        # Verify JSON schema of serialized DAGs.
        for json_str in serialized_dags.values():
            SerializedDAG.validate_schema(json_str)

        # Compares with the ground truth of JSON string.
        self.validate_serialized_dag(
            serialized_dags['simple_dag'],
            serialized_simple_dag_ground_truth)

    def validate_serialized_dag(self, json_dag, ground_truth_dag):
        """Verify serialized DAGs match the ground truth."""
        json_dag = json.loads(json_dag)

        self.assertTrue(
            json_dag[Encoding.VAR]['fileloc'].split('/')[-1] == 'test_dag_serialization.py')
        json_dag[Encoding.VAR]['fileloc'] = None
        json_dag[Encoding.VAR]['task_dict'][Encoding.VAR]['simple_task'][Encoding.VAR]['resources'] = None
        self.assertDictEqual(json_dag, ground_truth_dag)

    def test_deserialization(self):
        """A serialized DAG can be deserialized in another process."""
        queue = multiprocessing.Queue()
        proc = multiprocessing.Process(
            target=serialize_subprocess, args=(queue,))
        proc.daemon = True
        proc.start()

        stringified_dags = {}
        while True:
            v = queue.get()
            if v is None:
                break
            dag = Serialization.from_json(v)
            self.assertTrue(isinstance(dag, DAG))
            stringified_dags[dag.dag_id] = dag

        dags = collect_dags()
        self.assertTrue(set(stringified_dags.keys()) == set(dags.keys()))

        # Verify deserialized DAGs.
        example_skip_dag = stringified_dags['example_skip_dag']
        skip_operator_1_task = example_skip_dag.task_dict['skip_operator_1']
        self.validate_deserialized_task(
            skip_operator_1_task, 'DummySkipOperator', '#e8b7e4', '#000')

        # Verify that the DAG object has 'full_filepath' attribute
        # and is equal to fileloc
        self.assertTrue(hasattr(example_skip_dag, 'full_filepath'))
        self.assertEqual(example_skip_dag.full_filepath, example_skip_dag.fileloc)

        example_subdag_operator = stringified_dags['example_subdag_operator']
        section_1_task = example_subdag_operator.task_dict['section-1']
        self.validate_deserialized_task(
            section_1_task,
            SubDagOperator.__name__,
            SubDagOperator.ui_color,
            SubDagOperator.ui_fgcolor
        )

    def validate_deserialized_task(self, task, task_type, ui_color, ui_fgcolor):
        """Verify non-airflow operators are casted to BaseOperator."""
        self.assertTrue(isinstance(task, SerializedBaseOperator))
        # Verify the original operator class is recorded for UI.
        self.assertTrue(task.task_type == task_type)
        self.assertTrue(task.ui_color == ui_color)
        self.assertTrue(task.ui_fgcolor == ui_fgcolor)

        # Check that for Deserialised task, task.subdag is None for all other Operators
        # except for the SubDagOperator where task.subdag is an instance of DAG object
        if task.task_type == "SubDagOperator":
            self.assertIsNotNone(task.subdag)
            self.assertTrue(isinstance(task.subdag, DAG))
        else:
            self.assertIsNone(task.subdag)


if __name__ == '__main__':
    unittest.main()
