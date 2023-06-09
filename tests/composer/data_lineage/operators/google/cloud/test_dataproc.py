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

from unittest import mock
from unittest.mock import MagicMock, PropertyMock, call

import pytest
from google.api_core.exceptions import NotFound
from sqllineage.exceptions import SQLLineageException

from airflow import AirflowException
from airflow.composer.data_lineage.entities import DataprocMetastoreTable
from airflow.composer.data_lineage.operators import post_execute_prepare_lineage
from airflow.composer.data_lineage.operators.google.cloud.dataproc import (
    DataprocSQLJobLineageExtractor,
    ParsedSQLTable,
)
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

DATAPROC_PATH = "airflow.composer.data_lineage.operators.google.cloud.dataproc"
DATAPROC_LINEAGE_EXTRACTOR_PATH = DATAPROC_PATH + ".DataprocSQLJobLineageExtractor"

TEST_PROJECT_ID = "test_project_id"
TEST_LOCATION = "test_location"
TEST_METASTORE_INSTANCE_ID = "test_instance"
TEST_DATAPROC_CLUSTER = "test_cluster"

TEST_SCHEMA1 = "test_schema1"
TEST_SCHEMA2 = "test_schema2"
SQL_TABLE1 = "TABLE1"
SQL_TABLE2 = "TABLE2"
SQL_TABLE3 = "TABLE3"
SQL_SELECT_FROM_TABLE1 = f"SELECT * FROM {SQL_TABLE1}"
SQL_SELECT_FROM_TABLE2 = f"SELECT * FROM {SQL_TABLE2}"
SQL_SELECT_FROM_SCHEMA1_TABLE3 = f"SELECT * FROM {TEST_SCHEMA1}.{SQL_TABLE3}"
SQL_INSERT_INTO_TABLE1 = f"INSERT INTO {SQL_TABLE1}(col) VALUES(1)"
SQL_INSERT_INTO_TABLE2 = f"INSERT INTO {SQL_TABLE2}(col) VALUES(1)"
SQL_INSERT_INTO_SCHEMA1_TABLE3 = f"INSERT INTO {TEST_SCHEMA1}.{SQL_TABLE3}(col) VALUES(1)"
SQL_CREATE_TABLE1_AS_SELECT_TABLE2_JOIN_TABLE3 = (
    f"CREATE TABLE {SQL_TABLE1} AS "
    f"(SELECT test_column FROM {SQL_TABLE2} "
    f"INNER JOIN {SQL_TABLE3} ON {SQL_TABLE2}.test_column = {SQL_TABLE3}.test_column);"
)

HIVE_JOB = {
    "reference": {"project_id": TEST_PROJECT_ID},
    "placement": {"cluster_name": TEST_DATAPROC_CLUSTER},
    "hive_job": {"query_list": {"queries": [SQL_SELECT_FROM_TABLE1]}},
}


@pytest.fixture()
def hive_job():
    job = MagicMock()
    job.hive_job.query_list.queries = [SQL_SELECT_FROM_TABLE1]
    job.spark_sql_job = False
    job.presto_job = False
    job.trino_job = False
    job.placement.cluster_name = TEST_DATAPROC_CLUSTER
    return job


class TestDataprocSQLJobLineageExtractor:
    def test_data_lineage(self, hive_job):
        source_tables, target_tables = [MagicMock()], [MagicMock()]
        expected_sources, expected_targets = [MagicMock()], [MagicMock()]

        mock_parse_queries = MagicMock(return_value=(source_tables, target_tables))
        mock_build_lineage_entities = MagicMock(side_effect=[expected_sources, expected_targets])

        extractor = DataprocSQLJobLineageExtractor(
            job=hive_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        with mock.patch.multiple(
            extractor, parse_queries=mock_parse_queries, _build_lineage_entities=mock_build_lineage_entities
        ):
            actual_sources, actual_targets = extractor.data_lineage()

        assert actual_sources == expected_sources
        assert actual_targets == expected_targets
        mock_parse_queries.assert_called_once()
        mock_build_lineage_entities.assert_has_calls([call(tables=source_tables), call(tables=target_tables)])

    @pytest.mark.parametrize(
        "parsed_tables, expected_tables",
        [
            ([], []),
            (
                [ParsedSQLTable(TEST_SCHEMA1, SQL_TABLE1)],
                [
                    DataprocMetastoreTable(
                        project_id=TEST_PROJECT_ID,
                        location=TEST_LOCATION,
                        instance_id=TEST_METASTORE_INSTANCE_ID,
                        database=TEST_SCHEMA1,
                        table=SQL_TABLE1,
                    )
                ],
            ),
            (
                [ParsedSQLTable(TEST_SCHEMA1, SQL_TABLE1), ParsedSQLTable(TEST_SCHEMA2, SQL_TABLE2)],
                [
                    DataprocMetastoreTable(
                        project_id=TEST_PROJECT_ID,
                        location=TEST_LOCATION,
                        instance_id=TEST_METASTORE_INSTANCE_ID,
                        database=TEST_SCHEMA1,
                        table=SQL_TABLE1,
                    ),
                    DataprocMetastoreTable(
                        project_id=TEST_PROJECT_ID,
                        location=TEST_LOCATION,
                        instance_id=TEST_METASTORE_INSTANCE_ID,
                        database=TEST_SCHEMA2,
                        table=SQL_TABLE2,
                    ),
                ],
            ),
        ],
    )
    @mock.patch(
        DATAPROC_LINEAGE_EXTRACTOR_PATH + ".metastore_instance_id",
        new_callable=PropertyMock(return_value=TEST_METASTORE_INSTANCE_ID),
    )
    def test_build_lineage_entities(self, mock_metastore_instance, parsed_tables, expected_tables):
        extractor = DataprocSQLJobLineageExtractor(
            job=hive_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        actual_tables = extractor._build_lineage_entities(tables=parsed_tables)

        assert actual_tables == expected_tables

    @mock.patch(DATAPROC_LINEAGE_EXTRACTOR_PATH + ".dataproc_cluster", new_callable=PropertyMock)
    def test_metastore_instance_id(self, mock_dataproc_cluster):
        mock_dataproc_cluster.return_value.config.metastore_config.dataproc_metastore_service = (
            f"test/metastore/fqn/prefix/{TEST_METASTORE_INSTANCE_ID}"
        )

        extractor = DataprocSQLJobLineageExtractor(
            job=hive_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        actual_instance = extractor.metastore_instance_id

        assert actual_instance == TEST_METASTORE_INSTANCE_ID

    @mock.patch(DATAPROC_LINEAGE_EXTRACTOR_PATH + ".dataproc_cluster", new_callable=PropertyMock)
    def test_metastore_instance_id_no_config(self, mock_dataproc_cluster):
        mock_dataproc_cluster.return_value.config = None

        extractor = DataprocSQLJobLineageExtractor(
            job=hive_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        with pytest.raises(AirflowException):
            _ = extractor.metastore_instance_id

    @mock.patch(DATAPROC_LINEAGE_EXTRACTOR_PATH + ".dataproc_cluster", new_callable=PropertyMock)
    def test_metastore_instance_id_no_metastore(self, mock_dataproc_cluster):
        mock_dataproc_cluster.return_value.config.metastore_config = None

        extractor = DataprocSQLJobLineageExtractor(
            job=hive_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        with pytest.raises(AirflowException):
            _ = extractor.metastore_instance_id

    @mock.patch(DATAPROC_PATH + ".ClientOptions")
    @mock.patch(DATAPROC_PATH + ".ClusterControllerClient")
    @mock.patch(DATAPROC_PATH + ".GetClusterRequest")
    def test_dataproc_metastore_cluster(self, mock_request, mock_client, mock_client_options, hive_job):
        expected_client_options = MagicMock()
        expected_cluster = MagicMock()
        mock_client_options.return_value = expected_client_options
        mock_client.return_value.get_cluster.return_value = expected_cluster

        extractor = DataprocSQLJobLineageExtractor(
            job=hive_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        actual_cluster_1 = extractor.dataproc_cluster
        actual_cluster_2 = extractor.dataproc_cluster

        assert actual_cluster_1 == expected_cluster
        assert actual_cluster_2 == expected_cluster

        mock_client_options.assert_called_once_with(
            api_endpoint=f"{TEST_LOCATION}-dataproc.googleapis.com:443"
        )
        mock_client.assert_called_once_with(client_options=expected_client_options)
        mock_request.assert_called_once_with(
            project_id=TEST_PROJECT_ID, region=TEST_LOCATION, cluster_name=TEST_DATAPROC_CLUSTER
        )

    @mock.patch(DATAPROC_PATH + ".ClientOptions")
    @mock.patch(DATAPROC_PATH + ".ClusterControllerClient")
    @mock.patch(DATAPROC_PATH + ".GetClusterRequest")
    def test_dataproc_metastore_cluster_not_found(
        self, mock_request, mock_client, mock_client_options, hive_job
    ):
        mock_client.return_value.get_cluster.side_effect = NotFound(message="message")

        extractor = DataprocSQLJobLineageExtractor(
            job=hive_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        with pytest.raises(AirflowException):
            _ = extractor.dataproc_cluster

    @pytest.mark.parametrize(
        "mock_job",
        [
            MagicMock(
                hive_job=MagicMock(query_list=MagicMock(queries=[SQL_SELECT_FROM_TABLE1])),
                spark_sql_job=False,
                presto_job=False,
                trino_job=False,
            ),
            MagicMock(
                hive_job=False,
                spark_sql_job=MagicMock(query_list=MagicMock(queries=[SQL_SELECT_FROM_TABLE1])),
                presto_job=False,
                trino_job=False,
            ),
            MagicMock(
                hive_job=False,
                spark_sql_job=False,
                presto_job=MagicMock(query_list=MagicMock(queries=[SQL_SELECT_FROM_TABLE1])),
                trino_job=False,
            ),
            MagicMock(
                hive_job=False,
                spark_sql_job=False,
                presto_job=False,
                trino_job=MagicMock(query_list=MagicMock(queries=[SQL_SELECT_FROM_TABLE1])),
            ),
        ],
    )
    def test_get_queries(self, mock_job):
        extractor = DataprocSQLJobLineageExtractor(
            job=mock_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        assert extractor.get_queries() == [SQL_SELECT_FROM_TABLE1]

    def test_get_queries_unsupported_type(self):
        mock_job = MagicMock(hive_job=False, spark_sql_job=False, presto_job=False, trino_job=False)
        extractor = DataprocSQLJobLineageExtractor(
            job=mock_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        with pytest.raises(AirflowException):
            extractor.get_queries()

    @pytest.mark.parametrize(
        "queries, expected",
        [
            ([""], ([], [])),
            ([SQL_SELECT_FROM_TABLE1], ([], [])),
            ([f"{SQL_SELECT_FROM_TABLE1}; {SQL_SELECT_FROM_TABLE2}"], ([], [])),
            ([SQL_SELECT_FROM_TABLE1, SQL_SELECT_FROM_TABLE2], ([], [])),
            (
                [
                    f"{SQL_SELECT_FROM_TABLE1}; {SQL_INSERT_INTO_TABLE1}",
                    f"{SQL_SELECT_FROM_TABLE2}; {SQL_INSERT_INTO_TABLE2}",
                ],
                ([], []),
            ),
            ([SQL_INSERT_INTO_TABLE1], ([], [])),
            ([f"{SQL_INSERT_INTO_TABLE1}; {SQL_INSERT_INTO_TABLE2}"], ([], [])),
            ([SQL_INSERT_INTO_TABLE1, SQL_INSERT_INTO_TABLE2], ([], [])),
            (
                [SQL_CREATE_TABLE1_AS_SELECT_TABLE2_JOIN_TABLE3],
                (
                    [ParsedSQLTable("default", SQL_TABLE2), ParsedSQLTable("default", SQL_TABLE3)],
                    [ParsedSQLTable("default", SQL_TABLE1)],
                ),
            ),
        ],
    )
    def test_parse_queries(self, queries, expected, hive_job):
        hive_job.hive_job.query_list.queries = queries
        extractor = DataprocSQLJobLineageExtractor(
            job=hive_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        assert extractor.parse_queries() == expected

    @pytest.mark.parametrize(
        "queries, expected",
        [
            ([SQL_SELECT_FROM_SCHEMA1_TABLE3], ([], [])),
            ([SQL_INSERT_INTO_SCHEMA1_TABLE3], ([], [])),
            (
                [SQL_CREATE_TABLE1_AS_SELECT_TABLE2_JOIN_TABLE3],
                (
                    [ParsedSQLTable("default", SQL_TABLE2), ParsedSQLTable("default", SQL_TABLE3)],
                    [ParsedSQLTable("default", SQL_TABLE1)],
                ),
            ),
        ],
    )
    def test_parse_queries_default_schema(self, queries, expected, hive_job):
        hive_job.hive_job.query_list.queries = queries
        extractor = DataprocSQLJobLineageExtractor(
            job=hive_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        assert extractor.parse_queries() == expected

    @mock.patch("sqlparse.split")
    def test_parse_queries_type_error(self, mock_split, hive_job):
        hive_job.hive_job.query_list.queries = [SQL_SELECT_FROM_TABLE1]
        mock_split.side_effect = TypeError

        extractor = DataprocSQLJobLineageExtractor(
            job=hive_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        with pytest.raises(AirflowException):
            extractor.parse_queries()

    @pytest.mark.parametrize("exception", [SQLLineageException, IndexError])
    @mock.patch(f"{DATAPROC_PATH}.LineageRunner.source_tables", new_callable=PropertyMock)
    def test_parse_queries_sqllineage_source_tables_exception(self, mock_source_tables, exception, hive_job):
        hive_job.hive_job.query_list.queries = [SQL_SELECT_FROM_TABLE1]
        mock_source_tables.side_effect = exception

        extractor = DataprocSQLJobLineageExtractor(
            job=hive_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        with pytest.raises(AirflowException):
            extractor.parse_queries()

    @pytest.mark.parametrize("exception", [SQLLineageException, IndexError])
    @mock.patch(f"{DATAPROC_PATH}.LineageRunner.target_tables", new_callable=PropertyMock)
    def test_parse_queries_sqllineage_target_tables_exception(self, mock_target_tables, exception, hive_job):
        hive_job.hive_job.query_list.queries = [SQL_SELECT_FROM_TABLE1]
        mock_target_tables.side_effect = exception

        extractor = DataprocSQLJobLineageExtractor(
            job=hive_job, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        with pytest.raises(AirflowException):
            extractor.parse_queries()


class TestDataprocSubmitJobOperatorLineageMixin:
    @mock.patch(DATAPROC_PATH + ".DataprocHook")
    @mock.patch(DATAPROC_PATH + ".DataprocSQLJobLineageExtractor")
    def test_post_execute_prepare_lineage(self, mock_extractor, mock_hook):
        expected_inlets = [MagicMock(), MagicMock()]
        expected_outlets = [MagicMock(), MagicMock()]
        mock_extractor.return_value.data_lineage.return_value = (expected_inlets, expected_outlets)

        task = DataprocSubmitJobOperator(
            task_id="hive_task", job=HIVE_JOB, region=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )
        post_execute_prepare_lineage(task=task, context={})

        assert task.inlets == expected_inlets
        assert task.outlets == expected_outlets

    @mock.patch(DATAPROC_PATH + ".DataprocHook")
    def test_post_execute_prepare_lineage_no_project_id(self, mock_hook):
        mock_hook.return_value.project_id = None

        task = DataprocSubmitJobOperator(task_id="hive_task", job=HIVE_JOB, region=TEST_LOCATION)
        post_execute_prepare_lineage(task=task, context={})

        assert task.inlets == []
        assert task.outlets == []

    @mock.patch(DATAPROC_PATH + ".DataprocHook")
    def test_post_execute_prepare_lineage_no_job(self, mock_hook):
        m_hook = MagicMock(project_id=TEST_PROJECT_ID)
        m_hook.get_job.side_effect = NotFound(message="message")
        mock_hook.return_value = m_hook

        task = DataprocSubmitJobOperator(
            task_id="hive_task", job=HIVE_JOB, region=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )
        post_execute_prepare_lineage(task=task, context={})

        assert task.inlets == []
        assert task.outlets == []

    @mock.patch(DATAPROC_PATH + ".DataprocHook")
    @mock.patch(DATAPROC_PATH + ".DataprocSQLJobLineageExtractor")
    def test_post_execute_prepare_lineage_no_inlets(self, mock_extractor, mock_hook):
        inlets = []
        outlets = [MagicMock(), MagicMock()]
        mock_extractor.return_value.data_lineage.return_value = (inlets, outlets)

        task = DataprocSubmitJobOperator(
            task_id="hive_task", job=HIVE_JOB, region=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )
        post_execute_prepare_lineage(task=task, context={})

        assert task.inlets == []
        assert task.outlets == []

    @mock.patch(DATAPROC_PATH + ".DataprocHook")
    @mock.patch(DATAPROC_PATH + ".DataprocSQLJobLineageExtractor")
    def test_post_execute_prepare_lineage_no_outlets(self, mock_extractor, mock_hook):
        inlets = [MagicMock(), MagicMock()]
        outlets = []
        mock_extractor.return_value.data_lineage.return_value = (inlets, outlets)

        task = DataprocSubmitJobOperator(
            task_id="hive_task", job=HIVE_JOB, region=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )
        post_execute_prepare_lineage(task=task, context={})

        assert task.inlets == []
        assert task.outlets == []

    @mock.patch(DATAPROC_PATH + ".DataprocHook")
    @mock.patch(DATAPROC_PATH + ".DataprocSQLJobLineageExtractor")
    def test_post_execute_prepare_lineage_airflow_exception(self, mock_extractor, mock_hook):
        mock_extractor.return_value.data_lineage.side_effect = AirflowException

        task = DataprocSubmitJobOperator(
            task_id="hive_task", job=HIVE_JOB, region=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )
        post_execute_prepare_lineage(task=task, context={})

        assert task.inlets == []
        assert task.outlets == []
