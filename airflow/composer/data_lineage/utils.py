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
from __future__ import annotations

import hashlib
import os
import uuid
from typing import TYPE_CHECKING, Any, TypeVar

from sqllineage.runner import LineageRunner

from airflow.models import TaskInstance

if TYPE_CHECKING:
    from sqllineage.core.models import Table
    from sqlparse.sql import Statement

from airflow.composer.data_lineage.entities import BigQueryTable

LOCATION_PATH = f"projects/{os.environ.get('GCP_PROJECT')}/locations/{os.environ.get('COMPOSER_LOCATION')}"


def generate_uuid_from_string(s: str) -> str:
    """Returns string representation of UUID generated from given string."""
    md5_hash = hashlib.md5()
    md5_hash.update(s.encode("utf-8"))
    return str(uuid.UUID(md5_hash.hexdigest()))


def get_process_id(environment_name: str, dag_id: str, task_id: str) -> str:
    """Returns lineage process id generated from given parameters.

    Airflow task corresponds to Data Lineage Process, therefore Composer environment name,
    DAG id and task id uniquely identify Process.

    Returns:
        Suffix for full Process name "projects/{project}/locations/{location}/processes/{process}".
    """
    uuid1 = generate_uuid_from_string(environment_name)
    uuid2 = generate_uuid_from_string(dag_id)
    uuid3 = generate_uuid_from_string(task_id)

    return generate_uuid_from_string(uuid1 + uuid2 + uuid3)


def get_run_id(task_instance_run_id: str) -> str:
    """Returns lineage run id generated from given parameter.

    Airflow task_instance corresponds to Data Lineage Run, therefore task_instance.run_id
    uniquely identifies Run.

    Returns:
        Suffix for full Run name "projects/{project}/locations/{location}/processes/{process}/runs/{run}".
    """
    return generate_uuid_from_string(task_instance_run_id)


T = TypeVar("T")


def _build_BigQueryTable(source_table: Table, default_dataset: str, default_project: str) -> BigQueryTable:
    from sqllineage.core.models import Schema

    table = source_table.raw_name
    dataset_id, project_id = default_dataset, default_project
    schema = source_table.schema
    if schema != Schema():
        table_prefix = schema.raw_name.split(".")
        if len(table_prefix) == 1:
            dataset_id = table_prefix[0]
        else:
            project_id = table_prefix[0]
            dataset_id = table_prefix[1]
    return BigQueryTable(project_id=project_id, dataset_id=dataset_id, table_id=table)


def is_big_query_table_in_sources(
    query: str, outlet: BigQueryTable, default_dataset: str | None, default_project: str | None
) -> bool:
    runner = LineageRunner(sql=query, dialect="bigquery")
    for source_table in runner.source_tables:
        source = _build_BigQueryTable(source_table, default_dataset or "", default_project or "")
        if source == outlet:
            return True
    return False


def exclude_outlet(inlets: list[T], outlet: T) -> list[T]:
    """Excludes outlet from the given list of inlets.

    Args:
        inlets: List of inlets.
        outlet: Outlet that must be excluded from the inlets list.

    Returns:
        Copy of the given list of inlets without given outlet.
    """
    return [_inlet for _inlet in inlets if _inlet != outlet]


def parsed_sql_statements(sql: str) -> list[Statement]:
    """Parses SQL query into a list of Statements.

    Args:
        sql: SQL query.

    Returns:
        list of objects representing Statements.
    """
    import sqlparse

    return [
        s
        for s in sqlparse.parse(
            sqlparse.format(sql.strip(), encoding=None, strip_comments=True), encoding=None
        )
        if s.token_first(skip_cm=True)
    ]


def xcom_pull(task_instance: TaskInstance, key: str | None = None) -> Any:
    """Pull data from xcom.

    Args:
        task_instance: Task instance.
        key: Key to pull data from.

    Returns:
        value from xcom.
    """
    kwargs = dict(task_ids=task_instance.task_id)
    if key is not None:
        kwargs["key"] = key
    if task_instance.map_index == -1:
        return task_instance.xcom_pull(**kwargs)
    return task_instance.xcom_pull(**kwargs)[task_instance.map_index]


def exclude_bigquery_partition(table_id: str) -> str:
    """Exclude partition from the BigQuery table id."""
    return table_id.split("$")[0]
