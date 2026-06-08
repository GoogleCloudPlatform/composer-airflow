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

from airflow.composer.patches.lineage.entities import (
    BigQueryTable,
    DataprocMetastoreTable,
    GCSEntity,
    MySQLTable,
    PostgresTable,
)


class TestEntities:
    def test_BigQueryTable_dataset(self):
        big_query_table = BigQueryTable(
            project_id="test-project", dataset_id="test-dataset", table_id="test-table"
        )

        assert big_query_table.namespace == "bigquery"
        assert big_query_table.name == "test-project.test-dataset.test-table"

    def test_GCSEntity_dataset(self):
        gcs_entity = GCSEntity(bucket="test-bucket", path="path/to/file.txt")

        assert gcs_entity.namespace == "gs://test-bucket"
        assert gcs_entity.name == "path/to/file.txt"

    def test_MySQLTable_dataset(self):
        mysql_table = MySQLTable(host="localhost", port="3306", schema="test_schema", table="test_table")

        assert mysql_table.namespace == "mysql://localhost:3306"
        assert mysql_table.name == "test_schema.test_table"

    def test_PostgresTable_dataset(self):
        postgres_table = PostgresTable(
            host="localhost", port="5432", database="test_db", schema="public", table="test_table"
        )

        assert postgres_table.namespace == "postgres://localhost:5432"
        assert postgres_table.name == "test_db.public.test_table"

    def test_DataprocMetastoreTable_dataset(self):
        dataproc_metastore_table = DataprocMetastoreTable(
            project_id="test-project",
            location="us-central1",
            instance_id="test-instance",
            database="test_db",
            table="test_table",
        )

        assert dataproc_metastore_table.namespace == "custom:dataproc_metastore"
        assert dataproc_metastore_table.name == "test-project.us-central1.test-instance.test_db.test_table"
