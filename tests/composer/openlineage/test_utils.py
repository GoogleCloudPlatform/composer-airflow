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
from airflow.composer.openlineage.utils import sanitize_display_name, _traverse_copy

class TestUtils:
    def test_sanitize_display_name(self):
        actual_sanitized_display_name = sanitize_display_name(
            "Composer Airflow task dag_id.task+17*_0-9 :&" + ("X" * 300)
        )

        expected_sanitized_display_name = "Composer Airflow task dag_id.task17_0-9 :&" + ("X" * 158)
        assert actual_sanitized_display_name == expected_sanitized_display_name

    def test_traverse_copy(self):
        original_event = {
            "eventType": "COMPLETE",
            "inputs": [],
            "job": {
                "facets": {
                    "gcp_lineage": {
                        "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.0.0",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/GcpLineageJobFacet.json#/$defs/GcpLineageJobFacet",
                        "displayName": "Composer Airflow Task composer-env.test-dag-id.insert_query_job",
                        "origin": {
                            "name": "projects/project_id/locations/us-central1/environments/composer-env",
                            "sourceType": "COMPOSER",
                        },
                    },
                    "jobType": {
                        "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.0.0",
                        "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                        "integration": "AIRFLOW",
                        "jobType": "TASK",
                        "processingType": "BATCH",
                    },
                    "sql": {
                        "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.0.0",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json#/$defs/SQLJobFacet",
                        "query": "SQL QUERY",
                    },
                },
                "name": "test-dag-id.insert_query_job",
                "namespace": "composer-env",
            },
            "outputs": [
                {
                    "facets": {
                        "schema": {
                            "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.0.0",
                            "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                            "fields": [
                                {"fields": [], "name": "value", "type": "INTEGER"},
                            ],
                        }
                    },
                    "name": "project_id.dataset-test.table1",
                    "namespace": "bigquery",
                    "outputFacets": {
                        "outputStatistics": {
                            "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.0.0",
                            "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/OutputStatisticsOutputDatasetFacet.json#/$defs/OutputStatisticsOutputDatasetFacet",
                            "rowCount": 2,
                            "size": 0,
                        }
                    },
                },
                {
                    "name": "project_id.dataset-test.table2",
                    "namespace": "bigquery",
                },
            ],
            "producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.0.0",
            "run": {
                "facets": {
                    "airflow": {
                        "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.0.0",
                        "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                        "dag": {
                            "dag_id": "test-dag-id",
                            "fileloc": "/home/airflow/gcs/dags/bigquery_lineage_ephemeral.py",
                            "owner": "airflow",
                            "schedule_interval": "@once",
                            "start_date": "2021-01-01T00:00:00+00:00",
                            "tags": "['example', 'bigquery']",
                            "timetable": {},
                        },
                    },
                    "gcp_composer_run": {
                        "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.0.0",
                        "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                        "dagRunId": "manual__2025-01-22T13:52:52.319932+00:00",
                    },
                    "parent": {
                        "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.0.0",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json#/$defs/ParentRunFacet",
                        "job": {"name": "test-dag-id", "namespace": "composer-env"},
                        "run": {"runId": "01948e49-8c5f-7ca0-92b2-a6ebadeef076"},
                    },
                },
                "runId": "01948e49-8c5f-726a-9448-4068f3948178",
            },
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        }
        FIELDS_TO_LOG = {
            "eventTime": True,
            "eventType": True,
            "inputs": {
                "name": True,
                "namespace": True,
            },
            "outputs": {
                "name": True,
                "namespace": True,
            },
            "job": {
                "facets": {
                    "gcp_composer_job": True,
                    "gcp_lineage": True,
                    "jobType": {
                        "jobType": True,
                    },
                },
                "name": True,
                "namespace": True,
            },
            "run": {
                "facets":{
                    "gcp_composer_run": True
                }
            },
        }

        expected_copy_dict = {
            "eventType": "COMPLETE",
            "inputs": [],
            "job": {
                "facets": {
                    "gcp_lineage": {
                        "displayName": "Composer Airflow Task composer-env.test-dag-id.insert_query_job",
                        "origin": {
                            "name": "projects/project_id/locations/us-central1/environments/composer-env",
                            "sourceType": "COMPOSER",
                        },
                    },
                    "jobType": {"jobType": "TASK"},
                },
                "name": "test-dag-id.insert_query_job",
                "namespace": "composer-env",
            },
            "outputs": [
                {"name": "project_id.dataset-test.table1", "namespace": "bigquery"},
                {"name": "project_id.dataset-test.table2", "namespace": "bigquery"},
            ],
            "run": {
                "facets": {
                    "gcp_composer_run": {"dagRunId": "manual__2025-01-22T13:52:52.319932+00:00"},
                },
            },
        }

        copy_dict = _traverse_copy(original_event, FIELDS_TO_LOG)

        assert expected_copy_dict == copy_dict
