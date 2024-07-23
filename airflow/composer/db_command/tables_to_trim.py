#
# Copyright 2023 Google LLC
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

from airflow.jobs.job import Job
from airflow.models import (
    DagRun,
    ImportError,
    Log,
    RenderedTaskInstanceFields,
    SlaMiss,
    TaskInstance,
    TaskReschedule,
    XCom,
)


def compile_table_data(model_class, age_column, extra_data=None):
    """Helper function to create a table entry."""
    result = {"airflow_db_model": model_class, "age_column": age_column}
    if extra_data is not None:
        result.update(extra_data)
    return result


def tables_to_trim():
    """List of tables with primary keys for them and any additional information.

    It is very important to have a proper order of tables per Airflow version here. We want to avoid cascade
    removals of data from tables to avoid blocking more than one table at the time. Those tables might vary
    acros version of Airflow, thus we want to provide proper ordering for each of them.
    """
    tables = [
        compile_table_data(Job, Job.latest_heartbeat),
        compile_table_data(Log, Log.dttm),
        compile_table_data(
            SlaMiss, SlaMiss.execution_date, {"primary_key": [SlaMiss.task_id, SlaMiss.dag_id]}
        ),
        compile_table_data(ImportError, ImportError.timestamp),
        compile_table_data(
            XCom,
            XCom.execution_date,
            {"primary_key": [XCom.dag_run_id, XCom.task_id, XCom.map_index, XCom.key]},
        ),
        compile_table_data(
            RenderedTaskInstanceFields,
            RenderedTaskInstanceFields.execution_date,
            {
                "primary_key": [
                    RenderedTaskInstanceFields.dag_id,
                    RenderedTaskInstanceFields.task_id,
                    RenderedTaskInstanceFields.run_id,
                    RenderedTaskInstanceFields.map_index,
                ],
            },
        ),
        compile_table_data(TaskReschedule, TaskReschedule.execution_date),
        compile_table_data(
            TaskInstance,
            TaskInstance.execution_date,
            {
                "primary_key": [
                    TaskInstance.dag_id,
                    TaskInstance.task_id,
                    TaskInstance.run_id,
                    TaskInstance.map_index,
                ],
            },
        ),
        compile_table_data(DagRun, DagRun.execution_date, [("keep_last", True)]),
    ]
    return tables


def get_table_primary_key(table):
    if "primary_key" in table.keys():
        return table["primary_key"]
    else:
        return [table["airflow_db_model"].id]
