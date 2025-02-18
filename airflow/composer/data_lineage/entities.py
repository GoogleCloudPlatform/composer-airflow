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
"""Composer Data Lineage Airflow entity definitions."""
from __future__ import annotations

import attr
from typing import ClassVar

from airflow.providers.common.compat.openlineage.facet import Dataset


@attr.s(auto_attribs=True, kw_only=True, init=False)
class BigQueryTable(Dataset):
    """Airflow lineage entity representing BigQuery table."""

    project_id: str
    dataset_id: str
    table_id: str
    namespace: str = attr.ib(init=False)
    name: str = attr.ib(init=False)

    template_fields: ClassVar = ("project_id", "dataset_id", "table_id", "namespace", "name")

    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

        self.namespace = "bigquery"
        self.name = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        super().__init__(namespace=self.namespace, name=self.name)


@attr.s(auto_attribs=True, kw_only=True, init=False)
class DataLineageEntity(Dataset):
    """Airflow lineage entity representing generic Data Lineage entity."""

    fully_qualified_name: str = attr.ib()
    namespace: str = attr.ib(init=False)
    name: str = attr.ib(init=False)

    template_fields: ClassVar = ("fully_qualified_name", "namespace", "name")

    def __init__(self, fully_qualified_name: str):
        self.fully_qualified_name = fully_qualified_name

        try:
            self.namespace, self.name = fully_qualified_name.split(":")
            self.namespace = f"custom:{self.namespace}"
        except ValueError:
            self.namespace = "custom"
            self.name = fully_qualified_name

        super().__init__(namespace=self.namespace, name=self.name)


@attr.s(auto_attribs=True, kw_only=True, init=False)
class GCSEntity(Dataset):
    """Airflow lineage entity representing generic Cloud Storage entity."""

    bucket: str
    path: str
    namespace: str = attr.ib(init=False)
    name: str = attr.ib(init=False)

    template_fields: ClassVar = ("bucket", "path", "namespace", "name")

    def __init__(self, bucket: str, path: str):
        self.bucket = bucket
        self.path = path

        self.namespace = f"gs://{bucket}"
        self.name = path
        super().__init__(namespace=self.namespace, name=self.name)


@attr.s(auto_attribs=True, kw_only=True, init=False)
class MySQLTable(Dataset):
    """Airflow lineage entity representing MySQL table."""

    host: str
    port: str
    schema: str
    table: str
    namespace: str = attr.ib(init=False)
    name: str = attr.ib(init=False)

    template_fields: ClassVar = ("host", "port", "schema", "table", "namespace", "name")

    def __init__(self, host: str, port: str, schema: str, table: str):
        self.host = host
        self.port = port
        self.schema = schema
        self.table = table

        self.namespace = f"mysql://{host}:{port}"
        self.name = f"{schema}.{table}"
        super().__init__(namespace=self.namespace, name=self.name)


@attr.s(auto_attribs=True, kw_only=True, init=False)
class PostgresTable(Dataset):
    """Airflow lineage entity representing Postgres table."""

    host: str
    port: str
    database: str
    schema: str
    table: str
    namespace: str = attr.ib(init=False)
    name: str = attr.ib(init=False)

    template_fields: ClassVar = ("host", "port", "database", "schema", "table", "namespace", "name")

    def __init__(self, host: str, port: str, database: str, schema: str, table: str):
        self.host = host
        self.port = port
        self.database = database
        self.schema = schema
        self.table = table

        self.namespace = f"postgres://{host}:{port}"
        self.name = f"{database}.{schema}.{table}"
        super().__init__(namespace=self.namespace, name=self.name)


@attr.s(auto_attribs=True, kw_only=True, init=False)
class DataprocMetastoreTable(Dataset):
    """Airflow lineage entity representing Dataproc Metastore table."""

    project_id: str
    location: str
    instance_id: str
    database: str
    table: str
    namespace: str = attr.ib(init=False)
    name: str = attr.ib(init=False)

    template_fields: ClassVar = ("project_id", "location", "instance_id", "database", "table", "namespace", "name")

    def __init__(self, project_id: str, location: str, instance_id: str, database: str, table: str):
        self.project_id = project_id
        self.location = location
        self.instance_id = instance_id
        self.database = database
        self.table = table

        self.namespace = "custom:dataproc_metastore"
        self.name = f"{project_id}.{location}.{instance_id}.{database}.{table}"
        super().__init__(namespace=self.namespace, name=self.name)
