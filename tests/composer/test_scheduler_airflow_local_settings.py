#
# Copyright 2020 Google LLC
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
import datetime
import os
import unittest
from multiprocessing import Lock, Manager

from airflow import settings
from airflow.composer.scheduler_airflow_local_settings import _dag_to_filepath, task_policy
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.security.permissions import ACTION_CAN_EDIT, ACTION_CAN_READ, RESOURCE_DAG_PREFIX
from airflow.www.app import cached_app
from tests.test_utils.config import conf_vars


class TestSchedulerAirflowLocalSettings(unittest.TestCase):
    @conf_vars({("webserver", "rbac_autoregister_per_folder_roles"): "True"})
    def test_task_policy(self):
        # Prepare security manager.
        appbuilder = cached_app().appbuilder  # pylint: disable=no-member
        appbuilder.sm.lock = Lock()
        appbuilder.sm.dag_to_role = Manager().dict()

        role_a_dag = DAG(
            dag_id='role_a_dag',
            full_filepath=os.path.join(settings.DAGS_FOLDER, 'role_a/dag.py'),
            start_date=datetime.datetime(2017, 1, 1),
        )
        role_a_task = DummyOperator(
            task_id='dummy',
            dag=role_a_dag,
        )
        root_dag = DAG(
            dag_id='root_dag',
            full_filepath=os.path.join(settings.DAGS_FOLDER, 'dag.py'),
            start_date=datetime.datetime(2017, 1, 1),
        )
        root_task = DummyOperator(
            task_id='dummy',
            dag=root_dag,
        )
        role_length_exceed_dag = DAG(
            dag_id='role_length_exceed_dag',
            full_filepath=os.path.join(settings.DAGS_FOLDER, 'role_{}/dag.py'.format('x' * 70)),
            start_date=datetime.datetime(2017, 1, 1),
        )
        role_length_exceed_task = DummyOperator(
            task_id='dummy',
            dag=role_length_exceed_dag,
        )

        task_policy(role_a_task)
        task_policy(root_task)
        task_policy(role_length_exceed_task)

        assert _dag_to_filepath == {
            'role_a_dag': role_a_dag.full_filepath,
            'root_dag': root_dag.full_filepath,
            'role_length_exceed_dag': role_length_exceed_dag.full_filepath,
        }

        role_a = appbuilder.sm.find_role('role_a')
        permissions = set()
        for perm in role_a.permissions:
            if perm.view_menu.name != f'{RESOURCE_DAG_PREFIX}role_a_dag':
                continue
            permissions.add(perm.permission.name)
        assert permissions == {ACTION_CAN_READ, ACTION_CAN_EDIT}
