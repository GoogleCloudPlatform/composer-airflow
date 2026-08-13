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

from airflow.jobs.job import Job
from airflow.models import (
    DagRun,
    Log,
    RenderedTaskInstanceFields,
    TaskInstance,
)
from airflow.models.errors import ParseImportError
from airflow.models.xcom import XComModel


def tables_to_trim():
    """
    Return list of tables to trim.

    Note: the order of tables in the list matter. Some tables depend on others, and should be cleaned up
    in a specific order.
    """
    tables = [
        _compile_table_data(Job, Job.latest_heartbeat),
        _compile_table_data(Log, Log.dttm),
        _compile_table_data(ParseImportError, ParseImportError.timestamp),
        _compile_table_data(
            XComModel,
            XComModel.logical_date,
            {"primary_key": [XComModel.dag_run_id, XComModel.task_id, XComModel.map_index, XComModel.key]},
        ),
        _compile_table_data(
            RenderedTaskInstanceFields,
            RenderedTaskInstanceFields.logical_date,
            {
                "primary_key": [
                    RenderedTaskInstanceFields.dag_id,
                    RenderedTaskInstanceFields.task_id,
                    RenderedTaskInstanceFields.run_id,
                    RenderedTaskInstanceFields.map_index,
                ],
            },
        ),
        _compile_table_data(
            TaskInstance,
            TaskInstance.logical_date,
            {
                "primary_key": [
                    TaskInstance.dag_id,
                    TaskInstance.task_id,
                    TaskInstance.run_id,
                    TaskInstance.map_index,
                ],
            },
        ),
        _compile_table_data(DagRun, DagRun.logical_date, [("keep_last", True)]),
    ]

    return tables


def get_table_primary_key(table):
    if "primary_key" in table:
        return table["primary_key"]
    return [table["airflow_db_model"].id]


def _compile_table_data(model_class, age_column, extra_data=None):
    result = {"airflow_db_model": model_class, "age_column": age_column}
    if extra_data:
        result.update(extra_data)

    return result
