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

import logging
from collections import namedtuple
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.openlineage.facet import Dataset

if TYPE_CHECKING:
    from google.cloud.dataproc_v1 import (
        Cluster,
        Job,
    )

    from airflow.models import TaskInstance

log = logging.getLogger(__name__)

ParsedSQLTable = namedtuple("ParsedSQLTable", "schema table")


class DataprocSQLJobLineageExtractor:
    """
    Class for extracting data lineage from Dataproc jobs with a SQL queries.

    The SQL queries include Hive, SparkSQL, Presto, Trino.
    """

    from collections.abc import Sequence
    from typing import TYPE_CHECKING

    def __init__(self, job: Job, project_id: str, location: str):
        self.job: Job = job
        self.project_id: str = project_id
        self.location: str = location
        self._dataproc_cluster: Cluster | None = None

    def data_lineage(
        self,
    ) -> tuple[
        list[Dataset] | None,
        list[Dataset] | None,
    ]:
        """
        Extract SQL queries from the Dataproc job and generates data lineage.

        Returns:
            Tuple consisting of two lists of Dataset.
            The first list contains source tables. The second list contains target tables.
        """
        source_tables, target_tables = self.parse_queries()
        return self._build_lineage_entities_as_dataset(
            tables=source_tables
        ), self._build_lineage_entities_as_dataset(tables=target_tables)

    def _build_lineage_entities_as_dataset(self, tables: list[ParsedSQLTable]) -> list[Dataset]:
        instance_id = self.metastore_instance_id
        return [
            Dataset(
                namespace="dataproc_metastore",
                name=".".join([self.project_id, self.location, instance_id, database, table]),
            )
            for database, table in tables
        ]

    @property
    def metastore_instance_id(self) -> str:
        cluster = self.dataproc_cluster
        config = cluster.config
        if not config:
            raise AirflowException(
                f"Dataproc cluster config wasn't set up for the cluster {cluster.name}. "
                f"Data Lineage wasn't reported."
            )
        metastore_config = config.metastore_config
        if metastore_config:
            return metastore_config.dataproc_metastore_service.split("/")[-1]

        raise AirflowException(
            f"Metastore service wasn't specified for the Dataproc cluster "
            f"{cluster.name}. Data lineage wasn't reported."
        )

    @property
    def dataproc_cluster(self) -> Cluster:
        from google.api_core.client_options import ClientOptions
        from google.api_core.exceptions import NotFound
        from google.cloud.dataproc_v1 import (
            ClusterControllerClient,
            GetClusterRequest,
        )

        if not self._dataproc_cluster:
            client_options = ClientOptions(api_endpoint=f"{self.location}-dataproc.googleapis.com:443")
            client = ClusterControllerClient(client_options=client_options)
            request = GetClusterRequest(
                project_id=self.project_id,
                region=self.location,
                cluster_name=self.job.placement.cluster_name,
            )
            try:
                self._dataproc_cluster = client.get_cluster(request=request)
            except NotFound:
                raise AirflowException(
                    f"Cluster {self.job.placement.cluster_name} not found. Data lineage wasn't reported."
                )
        return self._dataproc_cluster

    def get_queries(self) -> Sequence[str]:
        """
        Extract SQL queries from the Dataproc job.

        According to the documentation only one of the job fields, corresponding to a specific job type,
        is not empty. That's why we are looking for the first non-empty element among those fields that
        refer to SQL-based job types.
        https://cloud.google.com/python/docs/reference/dataproc/latest/google.cloud.dataproc_v1.types.Job.

        Returns:
            Sequence of SQL queries.
        """
        if TYPE_CHECKING:
            from google.cloud.dataproc_v1 import (
                HiveJob,
                PrestoJob,
                SparkJob,
                TrinoJob,
            )

        job_fields: list[HiveJob | SparkJob | PrestoJob | TrinoJob] = [
            self.job.hive_job,
            self.job.spark_sql_job,
            self.job.presto_job,
            self.job.trino_job,
        ]
        job_details = next((job_field for job_field in job_fields if job_field), None)
        if job_details:
            return job_details.query_list.queries
        raise AirflowException(
            f"The job with id {self.job.job_uuid} has unsupported type. Data lineage wasn't reported."
        )

    def parse_queries(
        self, default_schema: str = "default"
    ) -> tuple[list[ParsedSQLTable], list[ParsedSQLTable]]:
        """
        Parse SQL queries.

        Args:
            default_schema: default schema name when it is not specified in the SQL query.

        Returns:
            Tuple consisting of two lists of ParsedTable.
            The first list contains source tables. The second list contains target tables.

        Raises:
            AirflowException: if SQL parsing failed.
        """
        import sqlparse
        from sqllineage.exceptions import SQLLineageException
        from sqllineage.runner import LineageRunner

        if TYPE_CHECKING:
            from sqllineage.core.models import Table

        def _parsed_sql_table(table: Table) -> ParsedSQLTable:
            db = default_schema if table.schema.raw_name == table.schema.unknown else table.schema.raw_name
            return ParsedSQLTable(db, table.raw_name)

        source_tables: list[ParsedSQLTable] = []
        target_tables: list[ParsedSQLTable] = []
        for query in self.get_queries():
            try:
                sql_queries = sqlparse.split(query)
            except TypeError as ex:
                raise AirflowException(f"Error on splitting SQL queries: {ex}")

            for sql_query in sql_queries:
                lineage_runner = LineageRunner(sql=sql_query, dialect="ansi")

                try:
                    inlets, outlets = lineage_runner.source_tables, lineage_runner.target_tables
                except (SQLLineageException, IndexError) as ex:
                    raise AirflowException(f"Error on parsing query: {ex}")

                if inlets and outlets:
                    source_tables = [_parsed_sql_table(t) for t in inlets]
                    target_tables = [_parsed_sql_table(t) for t in outlets]
                    break
        return source_tables, target_tables


def xcom_pull(task_instance: TaskInstance, key: str | None = None) -> Any:
    """
    Pull data from xcom.

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
