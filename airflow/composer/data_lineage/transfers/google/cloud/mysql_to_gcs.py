#
# Copyright 2021 Google LLC
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
from urllib.parse import urlparse

import sqlparse
from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner

from airflow import AirflowException
from airflow.composer.data_lineage.entities import GCSEntity, MySQLTable
from airflow.composer.data_lineage.utils import parsed_sql_statements
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

log = logging.getLogger(__name__)


class MySQLToGCSOperatorLineageMixin:
    """Mixin class for MySQLToGCSOperator."""

    def post_execute_prepare_lineage(self: MySQLToGCSOperator, context: dict):  # type: ignore
        # 1. Parse connection URI
        try:
            hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        except AirflowException:
            log.exception("Error on MySqlHook creation")
            return

        try:
            uri = hook.get_uri()
        except AirflowNotFoundException:
            log.exception("Error on connection URI retrieving")
            return

        # URI examples - [user[:[password]]@]host[:port][/schema]
        parsed_uri = urlparse(uri)

        # 2. Parse SQL query
        try:
            sql_queries = sqlparse.split(self.sql)
        except TypeError:
            log.exception("Error on splitting SQL queries")
            return

        source_tables = None
        for query in sql_queries:
            is_select_statement = any(_s.get_type() == "SELECT" for _s in parsed_sql_statements(sql=query))
            if is_select_statement:
                try:
                    source_tables = LineageRunner(sql=query, dialect="mysql").source_tables
                    break
                except SQLLineageException:
                    log.exception("Error on parsing SQL query")
                    continue

        if not source_tables:
            log.info("No source tables detected in the SQL query")
            return

        self.inlets.extend(
            [
                MySQLTable(
                    host=parsed_uri.hostname,  # type: ignore
                    port=str(parsed_uri.port) if parsed_uri.port else "3306",
                    schema=parsed_uri.path.lstrip("/"),
                    table=_t.raw_name,
                )
                for _t in source_tables
            ]
        )
        self.outlets.append(GCSEntity(bucket=self.bucket, path=self.filename))
