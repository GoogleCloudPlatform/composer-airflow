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
import logging
from typing import Dict
from urllib.parse import urlparse

from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner

from airflow import AirflowException
from airflow.composer.data_lineage.entities import GCSEntity, MySQLTable
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.mysql.hooks.mysql import MySqlHook

log = logging.getLogger(__name__)


class MySQLToGCSOperatorLineageMixin:
    """Mixin class for MySQLToGCSOperator."""

    def post_execute_prepare_lineage(self, context: Dict):
        try:
            hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        except AirflowException:
            log.exception('Error on MySqlHook creation')
            return

        try:
            uri = hook.get_uri()
        except AirflowNotFoundException:
            log.exception('Error on connection URI retrieving')
            return

        # URI examples - [user[:[password]]@]host[:port][/schema]
        parsed_uri = urlparse(uri)

        try:
            source_tables = LineageRunner(sql=self.sql).source_tables
        except SQLLineageException:
            log.exception('Error on parsing SQL query')
            return

        if not source_tables:
            log.info('No source tables detected in the SQL query')
            return

        self.inlets.extend(
            [
                MySQLTable(
                    host=parsed_uri.hostname,
                    port=str(parsed_uri.port) if parsed_uri.port else '',
                    schema=parsed_uri.path.lstrip('/'),
                    table=_t.raw_name,
                )
                for _t in source_tables
            ]
        )
        self.outlets.append(GCSEntity(bucket=self.bucket, path=self.filename))
