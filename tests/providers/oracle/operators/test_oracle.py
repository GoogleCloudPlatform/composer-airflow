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
from __future__ import annotations

from random import randrange
from unittest import mock

import oracledb
import pendulum
import pytest

from airflow.models import DAG, DagModel, DagRun, TaskInstance
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.oracle.operators.oracle import OracleOperator, OracleStoredProcedureOperator
from airflow.utils.session import create_session
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType

DEFAULT_DATE = datetime(2017, 1, 1)


def create_context(task, persist_to_db=False, map_index=None):
    if task.has_dag():
        dag = task.dag
    else:
        dag = DAG(dag_id="dag", start_date=pendulum.now())
        dag.add_task(task)
    dag_run = DagRun(
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, DEFAULT_DATE),
        run_type=DagRunType.MANUAL,
        dag_id=dag.dag_id,
    )
    task_instance = TaskInstance(task=task, run_id=dag_run.run_id)
    task_instance.dag_run = dag_run
    if map_index is not None:
        task_instance.map_index = map_index
    if persist_to_db:
        with create_session() as session:
            session.add(DagModel(dag_id=dag.dag_id))
            session.add(dag_run)
            session.add(task_instance)
            session.commit()
    return {
        "dag": dag,
        "ts": DEFAULT_DATE.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "run_id": "test",
    }


class TestOracleOperator:
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_execute(self, mock_get_db_hook):
        sql = "SELECT * FROM test_table"
        oracle_conn_id = "oracle_default"
        parameters = {"parameter": "value"}
        autocommit = False
        context = "test_context"
        task_id = "test_task_id"

        with pytest.warns(DeprecationWarning, match="This class is deprecated.*"):
            operator = OracleOperator(
                sql=sql,
                oracle_conn_id=oracle_conn_id,
                parameters=parameters,
                autocommit=autocommit,
                task_id=task_id,
            )
        operator.execute(context=context)
        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql=sql,
            autocommit=autocommit,
            parameters=parameters,
            handler=fetch_all_handler,
            return_last=True,
            split_statements=False,
        )


class TestOracleStoredProcedureOperator:
    @mock.patch.object(OracleHook, "run", autospec=OracleHook.run)
    def test_execute(self, mock_run):
        procedure = "test"
        oracle_conn_id = "oracle_default"
        parameters = {"parameter": "value"}
        context = "test_context"
        task_id = "test_task_id"

        operator = OracleStoredProcedureOperator(
            procedure=procedure,
            oracle_conn_id=oracle_conn_id,
            parameters=parameters,
            task_id=task_id,
        )
        result = operator.execute(context=context)
        assert result is mock_run.return_value
        mock_run.assert_called_once_with(
            mock.ANY,
            "BEGIN test(:parameter); END;",
            autocommit=True,
            parameters=parameters,
            handler=mock.ANY,
        )

    @mock.patch.object(OracleHook, "callproc", autospec=OracleHook.callproc)
    def test_push_oracle_exit_to_xcom(self, mock_callproc):
        # Test pulls the value previously pushed to xcom and checks if it's the same
        procedure = "test_push"
        oracle_conn_id = "oracle_default"
        parameters = {"parameter": "value"}
        task_id = "test_push"
        ora_exit_code = "%05d" % randrange(10**5)
        task = OracleStoredProcedureOperator(
            procedure=procedure, oracle_conn_id=oracle_conn_id, parameters=parameters, task_id=task_id
        )
        context = create_context(task, persist_to_db=True)
        mock_callproc.side_effect = oracledb.DatabaseError(
            "ORA-" + ora_exit_code + ": This is a five-digit ORA error code"
        )
        try:
            task.execute(context=context)
        except oracledb.DatabaseError:
            assert task.xcom_pull(key="ORA", context=context, task_ids=[task_id])[0] == ora_exit_code
