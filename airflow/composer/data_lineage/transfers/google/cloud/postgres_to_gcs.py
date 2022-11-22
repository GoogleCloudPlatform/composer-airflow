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
import logging
from typing import Dict
from urllib.parse import urlparse

from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner

from airflow import AirflowException
from airflow.composer.data_lineage.entities import GCSEntity, PostgresTable
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)


class PostgresToGCSOperatorLineageMixin:
    """Mixin class for PostgresToGCSOperator."""

    def post_execute_prepare_lineage(self, context: Dict):
        try:
            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        except AirflowException as airflow_exception:
            log.exception(f"Error on creating hook: {airflow_exception}")
            return

        try:
            parsed_url = urlparse(hook.get_uri())
        except AirflowNotFoundException:
            log.exception("Error on parsing uri.")
            return

        host = parsed_url.hostname
        port = parsed_url.port if parsed_url.port else "5432"
        db_default = parsed_url.path[1:]

        try:
            source_tables = LineageRunner(self.sql).source_tables
        except SQLLineageException:
            log.exception("Error on parsing query.")
            return

        if not source_tables:
            log.info("No tables detected in the query.")
            return

        inlets = []
        for source_table in source_tables:
            table = source_table.raw_name

            db_schema_parts = source_table.schema.raw_name.split(".")

            if len(db_schema_parts) > 2:
                log.exception("Error on parsing schema identifier.")
                return

            database = db_schema_parts[0] if len(db_schema_parts) == 2 else db_default
            schema = db_schema_parts[-1]
            schema = schema if schema != "<default>" else "public"

            inlets.append(
                PostgresTable(
                    host=host,
                    port=port,
                    database=database,
                    schema=schema,
                    table=table,
                )
            )

        outlets = [GCSEntity(bucket=self.bucket, path=self.filename)]

        self.inlets.extend(inlets)
        self.outlets.extend(outlets)
