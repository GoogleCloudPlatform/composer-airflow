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

from sqlalchemy import exists
from sqlalchemy.ext.associationproxy import association_proxy

from airflow.jobs.job import Job
from airflow.models import (
    DagRun,
    Deadline,
    HITLDetail,
    Log,
    RenderedTaskInstanceFields,
    TaskInstance,
    TaskReschedule,
)
from airflow.models.asset import AssetEvent
from airflow.models.backfill import Backfill, BackfillDagRun
from airflow.models.errors import ParseImportError
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.models.xcom import XComModel


def _add_associations():
    """
    Dynamically adds association proxies to Airflow models for database retention.

    Some models (such as TaskInstanceHistory, Deadline, and BackfillDagRun) do not
    have a direct timestamp column indicating their expiration date, but are associated
    with a parent model (e.g. DagRun or Backfill) that does.

    Attaching SQLAlchemy `association_proxy` attributes allows us to reference parent
    timestamp fields (such as `logical_date` or `created_at`) directly, enabling standard
    `age_column < expiration_datetime` filtering without requiring custom join queries.
    """
    TaskInstanceHistory.db_trim_logical_date = association_proxy("dag_run", "logical_date")
    Deadline.db_trim_logical_date = association_proxy("dagrun", "logical_date")
    BackfillDagRun.db_trim_created_at = association_proxy("backfill", "created_at")


def tables_to_trim(trim_af3_tables: bool = False):
    """
    Return list of tables to trim.

    Note: the order of tables in the list matter. Some tables depend on others, and should be cleaned up
    in a specific order.
    """
    if trim_af3_tables:
        _add_associations()
        return [
            _compile_table_data(Job, Job.latest_heartbeat),
            _compile_table_data(Log, Log.dttm),
            _compile_table_data(ParseImportError, ParseImportError.timestamp),
            _compile_table_data(AssetEvent, AssetEvent.timestamp),
            _compile_table_data(
                XComModel,
                XComModel.logical_date,
                {
                    "primary_key": [
                        XComModel.dag_run_id,
                        XComModel.task_id,
                        XComModel.map_index,
                        XComModel.key,
                    ]
                },
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
                TaskReschedule,
                None,
                {
                    "custom_filter": lambda exp_dt: exists().where(
                        (TaskInstance.id == TaskReschedule.ti_id)
                        & (TaskInstance.dag_id == DagRun.dag_id)
                        & (TaskInstance.run_id == DagRun.run_id)
                        & (DagRun.logical_date < exp_dt)
                    )
                },
            ),
            _compile_table_data(
                TaskInstanceHistory,
                TaskInstanceHistory.db_trim_logical_date,
                {
                    "primary_key": [TaskInstanceHistory.task_instance_id],
                },
            ),
            _compile_table_data(
                HITLDetail,
                None,
                {
                    "primary_key": [HITLDetail.ti_id],
                    "custom_filter": lambda exp_dt: exists().where(
                        (TaskInstance.id == HITLDetail.ti_id)
                        & (TaskInstance.dag_id == DagRun.dag_id)
                        & (TaskInstance.run_id == DagRun.run_id)
                        & (DagRun.logical_date < exp_dt)
                    ),
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
            _compile_table_data(Deadline, Deadline.db_trim_logical_date),
            _compile_table_data(BackfillDagRun, BackfillDagRun.db_trim_created_at),
            _compile_table_data(DagRun, DagRun.logical_date, {"keep_last": True}),
            _compile_table_data(Backfill, Backfill.created_at),
        ]

    return [
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
        _compile_table_data(DagRun, DagRun.logical_date, {"keep_last": True}),
    ]


def get_table_primary_key(table):
    if "primary_key" in table:
        return table["primary_key"]
    return [table["airflow_db_model"].id]


def _compile_table_data(model_class, age_column, extra_data=None):
    result = {"airflow_db_model": model_class, "age_column": age_column}
    if extra_data:
        result.update(extra_data)

    return result
