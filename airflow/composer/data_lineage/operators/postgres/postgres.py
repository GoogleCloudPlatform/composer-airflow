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
import itertools
import logging
from typing import Dict, List
from urllib.parse import urlparse

import sqlparse
from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner

from airflow import AirflowException
from airflow.composer.data_lineage.entities import PostgresTable
from airflow.composer.data_lineage.exceptions import UnexpectedDbSchemaError
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)


class PostgresOperatorLineageMixin:
    """Mixin class for PostgresOperator."""

    def post_execute_prepare_lineage(self, context: Dict):
        try:
            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        except AirflowException as airflow_exception:
            log.exception(f"Error on creating hook: {airflow_exception}")
            return

        try:
            parsed_uri = urlparse(hook.get_uri())
        except AirflowNotFoundException:
            log.exception("Error on parsing uri.")
            return

        host = parsed_uri.hostname
        port = str(parsed_uri.port) if parsed_uri.port else "5432"
        db_default = parsed_uri.path[1:]

        try:
            if isinstance(self.sql, List):
                sql_queries = list(itertools.chain(*[sqlparse.split(query) for query in self.sql]))
            elif isinstance(self.sql, str):
                sql_queries = sqlparse.split(self.sql)
            else:
                raise TypeError
        except TypeError:
            log.exception("Error on splitting SQL query.")
            return

        for query in sql_queries:
            lineage_runner = LineageRunner(sql=query)
            try:
                source_tables = lineage_runner.source_tables
                target_tables = lineage_runner.target_tables
            except SQLLineageException:
                log.exception("Error on parsing query.")
                return

            if source_tables and target_tables:
                try:
                    inlets = _map_to_postgres_tables(source_tables, db_default, host, port)
                    outlets = _map_to_postgres_tables(target_tables, db_default, host, port)
                except UnexpectedDbSchemaError:
                    log.exception("Error on parsing database, schema from source or target tables.")
                    return

                self.inlets.extend(inlets)
                self.outlets.extend(outlets)
                break


def _map_to_postgres_tables(lineage_tables: List, db_default: str, host: str, port: str) -> List:
    """Returns list of PostgresTable objects.

    Args:
        lineage_tables: List of sqllineage.core.models.Table
        db_default: Name of database which is defined in connection
        host: Name of host which is defined in connection
        port: Port defined in connection

    Returns:
        If database is specified in the 'lineage_tables' parameter, it will be overwritten.
        List of PostgresTable objects.
    """
    result_tables = []
    for lineage_table in lineage_tables:
        db_schema_parts = lineage_table.schema.raw_name.split(".")

        if len(db_schema_parts) > 2:
            raise UnexpectedDbSchemaError(f"Error on parsing schema identifier: {lineage_table}")

        database = db_schema_parts[0] if len(db_schema_parts) == 2 else db_default
        schema = db_schema_parts[-1]
        schema = schema if schema != "<default>" else "public"

        result_tables.append(
            PostgresTable(
                host=host,
                port=port,
                database=database,
                schema=schema,
                table=lineage_table.raw_name
            )
        )

    return result_tables
