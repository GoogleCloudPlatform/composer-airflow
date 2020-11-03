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

import datetime
import unittest

from airflow import settings
from airflow.models import DAG, TaskInstance as TI, clear_task_instances
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.models import DEFAULT_DATE
from tests.test_utils import db


class TestClearTasks(unittest.TestCase):

    def setUp(self) -> None:
        db.clear_db_runs()

    def tearDown(self):
        db.clear_db_runs()

    def test_clear_task_instances(self):
        dag = DAG('test_clear_task_instances', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task0 = DummyOperator(task_id='0', owner='test', dag=dag)
        task1 = DummyOperator(task_id='1', owner='test', dag=dag, retries=2)
        ti0 = TI(task=task0, execution_date=DEFAULT_DATE)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)

        dag.create_dagrun(
            execution_date=ti0.execution_date,
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti0.run()
        ti1.run()
        with create_session() as session:
            qry = session.query(TI).filter(
                TI.dag_id == dag.dag_id).all()
            clear_task_instances(qry, session, dag=dag)

        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # Next try to run will be try 2
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.max_tries, 1)
        self.assertEqual(ti1.try_number, 2)
        self.assertEqual(ti1.max_tries, 3)

    def test_clear_task_instances_without_task(self):
        dag = DAG('test_clear_task_instances_without_task', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task0 = DummyOperator(task_id='task0', owner='test', dag=dag)
        task1 = DummyOperator(task_id='task1', owner='test', dag=dag, retries=2)
        ti0 = TI(task=task0, execution_date=DEFAULT_DATE)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)

        dag.create_dagrun(
            execution_date=ti0.execution_date,
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti0.run()
        ti1.run()

        # Remove the task from dag.
        dag.task_dict = {}
        self.assertFalse(dag.has_task(task0.task_id))
        self.assertFalse(dag.has_task(task1.task_id))

        with create_session() as session:
            qry = session.query(TI).filter(
                TI.dag_id == dag.dag_id).all()
            clear_task_instances(qry, session)

        # When dag is None, max_tries will be maximum of original max_tries or try_number.
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # Next try to run will be try 2
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.max_tries, 1)
        self.assertEqual(ti1.try_number, 2)
        self.assertEqual(ti1.max_tries, 2)

    def test_clear_task_instances_without_dag(self):
        dag = DAG('test_clear_task_instances_without_dag', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task0 = DummyOperator(task_id='task_0', owner='test', dag=dag)
        task1 = DummyOperator(task_id='task_1', owner='test', dag=dag, retries=2)
        ti0 = TI(task=task0, execution_date=DEFAULT_DATE)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)

        dag.create_dagrun(
            execution_date=ti0.execution_date,
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti0.run()
        ti1.run()

        with create_session() as session:
            qry = session.query(TI).filter(
                TI.dag_id == dag.dag_id).all()
            clear_task_instances(qry, session)

        # When dag is None, max_tries will be maximum of original max_tries or try_number.
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # Next try to run will be try 2
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.max_tries, 1)
        self.assertEqual(ti1.try_number, 2)
        self.assertEqual(ti1.max_tries, 2)

    def test_dag_clear(self):
        dag = DAG('test_dag_clear', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task0 = DummyOperator(task_id='test_dag_clear_task_0', owner='test', dag=dag)
        ti0 = TI(task=task0, execution_date=DEFAULT_DATE)

        dag.create_dagrun(
            execution_date=ti0.execution_date,
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        # Next try to run will be try 1
        self.assertEqual(ti0.try_number, 1)
        ti0.run()
        self.assertEqual(ti0.try_number, 2)
        dag.clear()
        ti0.refresh_from_db()
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.state, State.NONE)
        self.assertEqual(ti0.max_tries, 1)

        task1 = DummyOperator(task_id='test_dag_clear_task_1', owner='test',
                              dag=dag, retries=2)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)
        self.assertEqual(ti1.max_tries, 2)
        ti1.try_number = 1
        # Next try will be 2
        ti1.run()
        self.assertEqual(ti1.try_number, 3)
        self.assertEqual(ti1.max_tries, 2)

        dag.clear()
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # after clear dag, ti2 should show attempt 3 of 5
        self.assertEqual(ti1.max_tries, 4)
        self.assertEqual(ti1.try_number, 3)
        # after clear dag, ti1 should show attempt 2 of 2
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.max_tries, 1)

    def test_dags_clear(self):
        # setup
        session = settings.Session()
        dags, tis = [], []
        num_of_dags = 5
        for i in range(num_of_dags):
            dag = DAG('test_dag_clear_' + str(i), start_date=DEFAULT_DATE,
                      end_date=DEFAULT_DATE + datetime.timedelta(days=10))
            ti = TI(task=DummyOperator(task_id='test_task_clear_' + str(i), owner='test',
                                       dag=dag),
                    execution_date=DEFAULT_DATE)

            dag.create_dagrun(
                execution_date=ti.execution_date,
                state=State.RUNNING,
                run_type=DagRunType.SCHEDULED,
            )
            dags.append(dag)
            tis.append(ti)

        # test clear all dags
        for i in range(num_of_dags):
            tis[i].run()
            self.assertEqual(tis[i].state, State.SUCCESS)
            self.assertEqual(tis[i].try_number, 2)
            self.assertEqual(tis[i].max_tries, 0)

        DAG.clear_dags(dags)

        for i in range(num_of_dags):
            tis[i].refresh_from_db()
            self.assertEqual(tis[i].state, State.NONE)
            self.assertEqual(tis[i].try_number, 2)
            self.assertEqual(tis[i].max_tries, 1)

        # test dry_run
        for i in range(num_of_dags):
            tis[i].run()
            self.assertEqual(tis[i].state, State.SUCCESS)
            self.assertEqual(tis[i].try_number, 3)
            self.assertEqual(tis[i].max_tries, 1)

        DAG.clear_dags(dags, dry_run=True)

        for i in range(num_of_dags):
            tis[i].refresh_from_db()
            self.assertEqual(tis[i].state, State.SUCCESS)
            self.assertEqual(tis[i].try_number, 3)
            self.assertEqual(tis[i].max_tries, 1)

        # test only_failed
        from random import randint
        failed_dag_idx = randint(0, len(tis) - 1)
        tis[failed_dag_idx].state = State.FAILED
        session.merge(tis[failed_dag_idx])
        session.commit()

        DAG.clear_dags(dags, only_failed=True)

        for i in range(num_of_dags):
            tis[i].refresh_from_db()
            if i != failed_dag_idx:
                self.assertEqual(tis[i].state, State.SUCCESS)
                self.assertEqual(tis[i].try_number, 3)
                self.assertEqual(tis[i].max_tries, 1)
            else:
                self.assertEqual(tis[i].state, State.NONE)
                self.assertEqual(tis[i].try_number, 3)
                self.assertEqual(tis[i].max_tries, 2)

    def test_operator_clear(self):
        dag = DAG('test_operator_clear', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        op1 = DummyOperator(task_id='bash_op', owner='test', dag=dag)
        op2 = DummyOperator(task_id='dummy_op', owner='test', dag=dag, retries=1)

        op2.set_upstream(op1)

        ti1 = TI(task=op1, execution_date=DEFAULT_DATE)
        ti2 = TI(task=op2, execution_date=DEFAULT_DATE)

        dag.create_dagrun(
            execution_date=ti1.execution_date,
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti2.run()
        # Dependency not met
        self.assertEqual(ti2.try_number, 1)
        self.assertEqual(ti2.max_tries, 1)

        op2.clear(upstream=True)
        ti1.run()
        ti2.run(ignore_ti_state=True)
        self.assertEqual(ti1.try_number, 2)
        # max_tries is 0 because there is no task instance in db for ti1
        # so clear won't change the max_tries.
        self.assertEqual(ti1.max_tries, 0)
        self.assertEqual(ti2.try_number, 2)
        # try_number (0) + retries(1)
        self.assertEqual(ti2.max_tries, 1)
