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
import attr


@attr.s(auto_attribs=True, kw_only=True)
class BigQueryTable:
    """Airflow lineage entity representing BigQuery table."""

    project_id: str = attr.ib()
    dataset_id: str = attr.ib()
    table_id: str = attr.ib()


@attr.s(auto_attribs=True, kw_only=True)
class DataLineageEntity:
    """Airflow lineage entity representing generic Data Lineage entity."""

    fully_qualified_name: str = attr.ib()


@attr.s(auto_attribs=True, kw_only=True)
class GCSEntity:
    """Airflow lineage entity representing generic Cloud Storage entity."""

    bucket: str = attr.ib()
    path: str = attr.ib()
