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

import logging
from collections import namedtuple
from typing import Sequence

import sqlparse
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import NotFound
from google.cloud.dataproc_v1 import Cluster, ClusterControllerClient, GetClusterRequest, Job
from sqllineage.core.models import Table
from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner

from airflow import AirflowException
from airflow.composer.data_lineage.entities import DataprocMetastoreTable
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

log = logging.getLogger(__name__)


ParsedSQLTable = namedtuple("ParsedSQLTable", "schema table")


class DataprocSQLJobLineageExtractor:
    """
    Class for extracting data lineage from Dataproc jobs with a SQL queries, including
        Hive, SparkSQL, Presto, Trino.
    """

    def __init__(self, job: Job, project_id: str, location: str):
        self.job: Job = job
        self.project_id: str = project_id
        self.location: str = location
        self._dataproc_cluster: Cluster | None = None

    def data_lineage(self) -> tuple[list[DataprocMetastoreTable] | None, list[DataprocMetastoreTable] | None]:
        """Extracts SQL queries from the Dataproc job and generates data lineage.

        Returns:
            Tuple consisting of two lists of DataprocMetastoreTable.
            The first list contains source tables. The second list contains target tables.
        """
        source_tables, target_tables = self.parse_queries()
        return self._build_lineage_entities(tables=source_tables), self._build_lineage_entities(
            tables=target_tables
        )

    def _build_lineage_entities(self, tables: list[ParsedSQLTable]) -> list[DataprocMetastoreTable]:
        instance_id = self.metastore_instance_id
        return [
            DataprocMetastoreTable(
                project_id=self.project_id,
                location=self.location,
                instance_id=instance_id,
                database=database,
                table=table,
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
        """Extracts SQL queries from the Dataproc job.
        According to the documentation only one of the job fields, corresponding to a specific job type,
        is not empty. That's why we are looking for the first non-empty element among those fields that
        refer to SQL-based job types.
        https://cloud.google.com/python/docs/reference/dataproc/latest/google.cloud.dataproc_v1.types.Job

        Returns:
            Sequence of SQL queries.
        """
        job_fields = [
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
        """Parses SQL queries.

        Args:
            default_schema: default schema name when it is not specified in the SQL query.

        Returns:
            Tuple consisting of two lists of ParsedTable.
            The first list contains source tables. The second list contains target tables.

        Raises:
            AirflowException: if SQL parsing failed.
        """

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
                lineage_runner = LineageRunner(sql=sql_query)

                try:
                    inlets, outlets = lineage_runner.source_tables, lineage_runner.target_tables
                except (SQLLineageException, IndexError) as ex:
                    raise AirflowException(f"Error on parsing query: {ex}")

                if inlets and outlets:
                    source_tables = [_parsed_sql_table(t) for t in inlets]
                    target_tables = [_parsed_sql_table(t) for t in outlets]
                    break
        return source_tables, target_tables


class DataprocSubmitJobOperatorLineageMixin:
    """Mixin class for DataprocSubmitJobOperator."""

    def post_execute_prepare_lineage(self: DataprocSubmitJobOperator, context: dict):  # type: ignore
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        if hook.project_id is None:
            log.exception("The project_id is missing. Data lineage wasn't reported")
            return

        try:
            job = hook.get_job(job_id=self.job_id, project_id=self.project_id, region=self.region)
        except NotFound:
            log.exception(f"The job with id {self.job_id} wasn't found. Data lineage wasn't reported")
            return

        data_lineage_extractor = DataprocSQLJobLineageExtractor(
            job=job, project_id=hook.project_id, location=self.region
        )

        try:
            inlets, outlets = data_lineage_extractor.data_lineage()
            if not inlets:
                log.info("No sources were detected. Data lineage wasn't reported")
                return
            if not outlets:
                log.info("No targets were detected. Data lineage wasn't reported")
                return

            self.inlets.extend(inlets)
            self.outlets.extend(outlets)
        except AirflowException as ex:
            log.info(ex)
            return
