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

from airflow import DAG
from airflow.configuration import AIRFLOW_HOME
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync

with DAG(
    dag_id="dag_with_deferrable_operator",
    schedule="@once",
    start_date=datetime.datetime(2010, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    deferrable_task = DateTimeSensorAsync(
        dag=dag,
        task_id="deferrable",
        target_time="{{ dag_run.start_date + macros.timedelta(seconds=90) }}",
    )

    touch_file_task = BashOperator(
        dag=dag,
        task_id="touch_file",
        bash_command=f"touch {AIRFLOW_HOME}/dag_with_deferrable_operator_touch_file",
    )

    deferrable_task >> touch_file_task
