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

import datetime
import os
import pathlib

from airflow import DAG
from airflow.configuration import AIRFLOW_HOME
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def _python_touch_file():
    pathlib.Path(os.path.join(AIRFLOW_HOME, "basic_dag_python_touch_file")).touch()


with DAG(
    dag_id="basic_dag",
    schedule="@once",
    start_date=datetime.datetime(2010, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    PythonOperator(
        dag=dag,
        task_id="python_touch_file",
        python_callable=_python_touch_file,
    )

    BashOperator(
        dag=dag,
        task_id="bash_touch_file",
        bash_command=f"touch {AIRFLOW_HOME}/basic_dag_bash_touch_file",
    )
