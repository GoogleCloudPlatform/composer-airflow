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

import datetime
import unittest
from importlib import reload
from unittest import mock

from freezegun import freeze_time
from google.cloud.datacatalog.lineage_v1 import EntityReference, EventLink, LineageEvent, Origin, Process, Run
from parameterized import parameterized

from airflow.composer.data_lineage.adapter import ComposerDataLineageAdapter
from airflow.composer.data_lineage.entities import BigQueryTable, DataLineageEntity


class TestAdapter(unittest.TestCase):
    def test_get_entity_reference_big_query_table(self):
        adapter = ComposerDataLineageAdapter()
        big_query_table = BigQueryTable(
            project_id="test-project",
            dataset_id="test-dataset",
            table_id="test-table",
        )

        actual_entity_reference = adapter._get_entity_reference(big_query_table)

        expected_entity_reference = EntityReference(
            fully_qualified_name="bigquery:test-project.test-dataset.test-table",
        )
        self.assertEqual(actual_entity_reference, expected_entity_reference)

    def test_get_entity_reference_data_lineage_entity(self):
        adapter = ComposerDataLineageAdapter()
        data_lineage_entity = DataLineageEntity(
            fully_qualified_name="my_warehouse:test-fqn",
        )

        actual_entity_reference = adapter._get_entity_reference(data_lineage_entity)

        expected_entity_reference = EntityReference(
            fully_qualified_name="my_warehouse:test-fqn",
        )
        self.assertEqual(actual_entity_reference, expected_entity_reference)

    def test_get_entity_reference_unknown(self):
        adapter = ComposerDataLineageAdapter()

        # Pass empty dict to represent unknown Airflow entity.
        actual_entity_reference = adapter._get_entity_reference({})

        self.assertIsNone(actual_entity_reference)

    @mock.patch.dict("os.environ", {"COMPOSER_ENVIRONMENT": "environment-1"})
    @mock.patch("airflow.composer.data_lineage.utils.LOCATION_PATH", "projects/project-1")
    def test_construct_process(self):
        import airflow.composer.data_lineage.adapter

        # Reload adapter module to reevaluate COMPOSER_ENVIRONMENT_NAME const with environment variable.
        reload(airflow.composer.data_lineage.adapter)
        adapter = ComposerDataLineageAdapter()
        mock_task = mock.Mock(task_id="task-1", dag=mock.Mock(dag_id="dag-1"))

        actual_process = adapter._construct_process(mock_task)

        expected_process = Process(
            name="projects/project-1/processes/98de46aa-188e-23e0-6a5f-f0f5ed069b08",
            display_name="Composer Airflow task environment-1.dag-1.task-1",
            attributes={
                "composer_environment_name": "environment-1",
                "dag_id": "dag-1",
                "task_id": "task-1",
                "operator": "Mock",
            },
            origin=Origin(
                source_type=Origin.SourceType.COMPOSER,
                name="projects/project-1/environments/environment-1",
            ),
        )
        self.assertEqual(actual_process, expected_process)

    @freeze_time("2022-08-01 10:11:12")
    def test_construct_run(self):
        adapter = ComposerDataLineageAdapter()
        mock_task_instance = mock.Mock(
            run_id="test-run-id",
            start_date=datetime.datetime(2022, 8, 3, 1, 5, 7),
        )

        actual_run = adapter._construct_run(mock_task_instance, "test-process")

        expected_run = Run(
            name="test-process/runs/570e6350-1fd1-f8f1-e8cf-4b4d1976a8ea",
            display_name="Airflow task run test-run-id",
            attributes={
                "dag_run_id": "test-run-id",
            },
            start_time=datetime.datetime(2022, 8, 3, 1, 5, 7),
            end_time=datetime.datetime(2022, 8, 1, 10, 11, 12),
            state="COMPLETED",
        )
        self.assertEqual(actual_run, expected_run)

    @freeze_time("2022-08-01 10:11:12")
    def test_construct_lineage_events(self):
        def _get_big_query_table(table_id):
            return BigQueryTable(
                project_id="test-project",
                dataset_id="test-dataset",
                table_id=table_id,
            )

        def _get_entity_reference(table_id):
            return EntityReference(
                fully_qualified_name=f"bigquery:test-project.test-dataset.{table_id}",
            )

        adapter = ComposerDataLineageAdapter()
        big_query_table_1 = _get_big_query_table("test-table-1")
        big_query_table_2 = _get_big_query_table("test-table-2")
        big_query_table_3 = _get_big_query_table("test-table-3")
        entity_reference_1 = _get_entity_reference("test-table-1")
        entity_reference_2 = _get_entity_reference("test-table-2")
        entity_reference_3 = _get_entity_reference("test-table-3")

        actual_lineage_events = adapter._construct_lineage_events(
            inlets=[big_query_table_1, big_query_table_2],
            outlets=[big_query_table_2, big_query_table_3],
        )

        expected_lineage_events = [
            LineageEvent(
                links=[
                    EventLink(source=entity_reference_1, target=entity_reference_2),
                    EventLink(source=entity_reference_1, target=entity_reference_3),
                    EventLink(source=entity_reference_2, target=entity_reference_2),
                    EventLink(source=entity_reference_2, target=entity_reference_3),
                ],
                start_time=datetime.datetime(2022, 8, 1, 10, 11, 12),
                end_time=datetime.datetime(2022, 8, 1, 10, 11, 12),
            )
        ]
        self.assertEqual(actual_lineage_events, expected_lineage_events)

    @parameterized.expand(
        [
            ([], [{"unknown": True}]),
            ([{"unknown": True}], []),
        ]
    )
    def test_construct_lineage_events_unknown_entity(self, extra_inlets, extra_outlets):
        def _get_big_query_table(table_id):
            return BigQueryTable(
                project_id="test-project",
                dataset_id="test-dataset",
                table_id=table_id,
            )

        adapter = ComposerDataLineageAdapter()
        big_query_table_1 = _get_big_query_table("test-table-1")
        big_query_table_2 = _get_big_query_table("test-table-2")

        actual_lineage_events = adapter._construct_lineage_events(
            inlets=[big_query_table_1] + extra_inlets,
            outlets=[big_query_table_2] + extra_outlets,
        )

        expected_lineage_events = []
        self.assertEqual(actual_lineage_events, expected_lineage_events)

    def test_construct_lineage_events_no_inlets_outlets(self):
        adapter = ComposerDataLineageAdapter()

        actual_lineage_events = adapter._construct_lineage_events(
            inlets=[],
            outlets=[],
        )

        expected_lineage_events = []
        self.assertEqual(actual_lineage_events, expected_lineage_events)

    @freeze_time("2022-08-01 22:11:12")
    @mock.patch.dict("os.environ", {"COMPOSER_ENVIRONMENT": "environment-1"})
    @mock.patch("airflow.composer.data_lineage.utils.LOCATION_PATH", "projects/project-1")
    def test_get_lineage_events_bundle_on_task_completed(self):
        import airflow.composer.data_lineage.adapter

        # Reload adapter module to reevaluate COMPOSER_ENVIRONMENT_NAME const with environment variable.
        reload(airflow.composer.data_lineage.adapter)
        adapter = ComposerDataLineageAdapter()

        actual_lineage_events_bundle = adapter.get_lineage_events_bundle_on_task_completed(
            mock.Mock(
                task=mock.Mock(dag=mock.Mock(dag_id="dag-1"), task_id="task-1"),
                run_id="test-run-id",
                start_date=datetime.datetime(2022, 8, 1, 1, 2, 3),
            ),
            [
                BigQueryTable(
                    project_id="test-project",
                    dataset_id="test-dataset",
                    table_id="test-table-inlet",
                ),
            ],
            [
                BigQueryTable(
                    project_id="test-project",
                    dataset_id="test-dataset",
                    table_id="test-table-outlet",
                ),
            ],
        )

        expected_lineage_events_bundle = dict(
            process=Process(
                name="projects/project-1/processes/98de46aa-188e-23e0-6a5f-f0f5ed069b08",
                display_name="Composer Airflow task environment-1.dag-1.task-1",
                attributes={
                    "composer_environment_name": "environment-1",
                    "dag_id": "dag-1",
                    "task_id": "task-1",
                    "operator": "Mock",
                },
                origin=Origin(
                    source_type=Origin.SourceType.COMPOSER,
                    name="projects/project-1/environments/environment-1",
                ),
            ),
            run=Run(
                name=(
                    "projects/project-1/processes/98de46aa-188e-23e0-6a5f-f0f5ed069b08/"
                    "runs/570e6350-1fd1-f8f1-e8cf-4b4d1976a8ea"
                ),
                display_name="Airflow task run test-run-id",
                attributes={
                    "dag_run_id": "test-run-id",
                },
                start_time=datetime.datetime(2022, 8, 1, 1, 2, 3),
                end_time=datetime.datetime(2022, 8, 1, 22, 11, 12),
                state="COMPLETED",
            ),
            lineage_events=[
                LineageEvent(
                    links=[
                        EventLink(
                            source=EntityReference(
                                fully_qualified_name="bigquery:test-project.test-dataset.test-table-inlet",
                            ),
                            target=EntityReference(
                                fully_qualified_name="bigquery:test-project.test-dataset.test-table-outlet",
                            ),
                        ),
                    ],
                    start_time=datetime.datetime(2022, 8, 1, 22, 11, 12),
                    end_time=datetime.datetime(2022, 8, 1, 22, 11, 12),
                ),
            ],
        )
        self.assertEqual(actual_lineage_events_bundle, expected_lineage_events_bundle)

    def test_sanitize_display_name(self):
        adapter = ComposerDataLineageAdapter()

        actual_sanitized_display_name = adapter._sanitize_display_name(
            "Composer Airflow task dag_id.task+17*_0-9 :&" + ("X" * 300)
        )

        expected_sanitized_display_name = "Composer Airflow task dag_id.task17_0-9 :&" + ("X" * 158)
        self.assertEqual(actual_sanitized_display_name, expected_sanitized_display_name)
