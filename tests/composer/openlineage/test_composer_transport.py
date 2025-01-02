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

import os
import pytest
import uuid

from importlib import reload
from unittest import mock

from openlineage.client.client import Event
from openlineage.client.event_v2 import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import job_type_job, parent_run

from airflow.composer.openlineage.composer_transport import ComposerTransport, ComposerTransportConfig
from airflow.composer.openlineage.facets import ComposerJobFacet, ComposerRunFacet, GcpOrigin
from airflow.providers.openlineage.plugins.adapter import _PRODUCER
from airflow.providers.openlineage.plugins.facets import AirflowRunFacet, AirflowDagRunFacet
from airflow.version import version as airflow_version


TASK_START_EVENT = RunEvent(
    eventType=RunState.START,
    eventTime="2024-12-22T22:37:56.585453+00:00",
    run=Run(
        runId=str(uuid.uuid4()),
        facets={
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId=str(uuid.uuid4())),
                job=parent_run.Job(namespace="composer-env-name", name="test_dag_id"),
            ),
            "airflow": AirflowRunFacet(
                dag={
                    "dag_id": "test_dag_id",
                },
                dagRun={
                    "dag_id": "test_dag_id",
                    "run_id": "manual__2024-12-22T22:37:18.357507+00:00",
                    "start_date": "2024-12-22T22:37:18.976384+00:00",
                },
                taskInstance={
                    "queued_dttm": "2024-12-22T22:37:51.529730+00:00",
                },
                task={
                    "operator_class": "PythonOperator",
                    "task_id": "test_task_id",
                },
                taskUuid=str(uuid.uuid4()),
            ),
        },
    ),
    job=Job(
        namespace="composer-env-name",
        name="test_dag_id.test_task_id",
        facets={
            "jobType": job_type_job.JobTypeJobFacet(
                processingType="BATCH", integration="AIRFLOW", jobType="TASK"
            ),
        },
    ),
    producer=_PRODUCER,
    inputs=[],
    outputs=[],
)

TASK_COMPLETE_EVENT = RunEvent(
    eventType=RunState.COMPLETE,
    eventTime="2024-12-22T22:37:56.585453+00:00",
    run=Run(
        runId=str(uuid.uuid4()),
        facets={
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId=str(uuid.uuid4())),
                job=parent_run.Job(namespace="composer-env-name", name="test_dag_id"),
            ),
            "airflow": AirflowRunFacet(
                dag={
                    "dag_id": "test_dag_id",
                },
                dagRun={
                    "dag_id": "test_dag_id",
                    "run_id": "manual__2024-12-22T22:37:18.357507+00:00",
                    "start_date": "2024-12-22T22:37:18.976384+00:00",
                },
                taskInstance={
                    "queued_dttm": "2024-12-22T22:37:51.529730+00:00",
                },
                task={
                    "operator_class": "PythonOperator",
                    "task_id": "test_task_id",
                },
                taskUuid=str(uuid.uuid4()),
            ),
        },
    ),
    job=Job(
        namespace="composer-env-name",
        name="test_dag_id.test_task_id",
        facets={
            "jobType": job_type_job.JobTypeJobFacet(
                processingType="BATCH", integration="AIRFLOW", jobType="TASK"
            ),
        },
    ),
    producer=_PRODUCER,
    inputs=[
        Dataset(namespace="bigquery", name="a.b.c"),
        Dataset(namespace="bigquery", name="x.y.z"),
    ],
    outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
)

DAG_START_EVENT = RunEvent(
    eventType=RunState.START,
    eventTime="2024-12-22T23:10:02.401328+00:00",
    job=Job(
        namespace="composer-env-name",
        name="test_dag_id",
        facets={
            "jobType": job_type_job.JobTypeJobFacet(
                processingType="BATCH", integration="AIRFLOW", jobType="DAG"
            ),
        },
    ),
    run=Run(
        runId=str(uuid.uuid4()),
        facets={
            "airflowDagRun": AirflowDagRunFacet(
                dag={"test_dag_id"}, dagRun={"run_id": "manual__2024-12-22T22:37:18.357507+00:00"}
            )
        },
    ),
    inputs=[],
    outputs=[],
    producer=_PRODUCER,
)

DAG_COMPLETE_EVENT = RunEvent(
    eventType=RunState.COMPLETE,
    eventTime="2024-12-22T23:10:02.401328+00:00",
    job=Job(
        namespace="composer-env-name",
        name="test_dag_id",
        facets={
            "jobType": job_type_job.JobTypeJobFacet(
                processingType="BATCH", integration="AIRFLOW", jobType="DAG"
            ),
        },
    ),
    run=Run(
        runId=str(uuid.uuid4()),
    ),
    inputs=[],
    outputs=[],
    producer=_PRODUCER,
)


class TestComposerTransport:
    @mock.patch.dict("os.environ", {"COMPOSER_ENVIRONMENT": "composer-env-name"})
    @mock.patch.dict("os.environ", {"COMPOSER_VERSION": "composer-version"})
    @mock.patch.dict("os.environ", {"COMPOSER_ENVIRONMENT": "composer-env-name"})
    @pytest.mark.parametrize(
        "event, expected_metadata",
        [
            (TASK_START_EVENT, [("x-goog-ext-512598505-bin", b"\n\x08COMPOSER\x12\x04TASK")]),
            (TASK_COMPLETE_EVENT, [("x-goog-ext-512598505-bin", b"\n\x08COMPOSER\x12\x04TASK")]),
            (DAG_START_EVENT, [("x-goog-ext-512598505-bin", b"\n\x08COMPOSER\x12\x04DAG")]),
            (DAG_COMPLETE_EVENT, [("x-goog-ext-512598505-bin", b"\n\x08COMPOSER\x12\x04DAG")]),
        ],
    )
    def test_emit(self, event, expected_metadata):
        import airflow.composer.openlineage.composer_transport
        # Reload the transport module to reevaluate environment variables use in module level constants.
        reload(airflow.composer.openlineage.composer_transport)

        with mock.patch(
            "airflow.composer.openlineage.composer_transport.SyncLineageClient", autospec=True
        ) as mock_sync_lineage_client:
            transport = ComposerTransport(ComposerTransportConfig())
            transport.emit(event)

            mock_sync_lineage_client().process_open_lineage_run_event.assert_called_once_with(
                request=mock.ANY,
                metadata=expected_metadata,
                retry=mock.ANY,
            )
            assert (
                mock_sync_lineage_client().process_open_lineage_run_event.call_args_list[0][1]["retry"]._deadline
                == 5
            )

    @mock.patch.dict("os.environ", {"COMPOSER_ENVIRONMENT": "composer-env-name"})
    @mock.patch.dict("os.environ", {"COMPOSER_VERSION": "composer-version"})
    @mock.patch.dict("os.environ", {"COMPOSER_ENVIRONMENT": "composer-env-name"})
    @mock.patch.dict("os.environ", {"GCP_PROJECT": "test_gcp_project"})
    @mock.patch.dict("os.environ", {"COMPOSER_LOCATION": "us-central1"})
    @pytest.mark.parametrize(
        "event, expected_composer_job_facet, expected_composer_run_facet",
        [
            (
                TASK_START_EVENT,
                ComposerJobFacet(
                    environmentName="composer-env-name",
                    composerVersion="composer-version",
                    airflowVersion=airflow_version,
                    dagId="test_dag_id",
                    taskId="test_task_id",
                    operator="PythonOperator",
                ),
                ComposerRunFacet(dagRunId="manual__2024-12-22T22:37:18.357507+00:00"),
            ),
            (
                TASK_COMPLETE_EVENT,
                ComposerJobFacet(
                    environmentName="composer-env-name",
                    composerVersion="composer-version",
                    airflowVersion=airflow_version,
                    dagId="test_dag_id",
                    taskId="test_task_id",
                    operator="PythonOperator",
                ),
                ComposerRunFacet(dagRunId="manual__2024-12-22T22:37:18.357507+00:00"),
            ),
            (
                DAG_START_EVENT,
                ComposerJobFacet(
                    environmentName="composer-env-name",
                    composerVersion="composer-version",
                    airflowVersion=airflow_version,
                    dagId="test_dag_id",
                    taskId=None,
                    operator=None,
                ),
                ComposerRunFacet(dagRunId="manual__2024-12-22T22:37:18.357507+00:00"),
            ),
            (
                DAG_COMPLETE_EVENT,
                ComposerJobFacet(
                    environmentName="composer-env-name",
                    composerVersion="composer-version",
                    airflowVersion=airflow_version,
                    dagId="test_dag_id",
                    taskId=None,
                    operator=None,
                ),
                ComposerRunFacet(dagRunId=None),
            ),
        ],
    )
    def test_patch_event(self, event, expected_composer_job_facet, expected_composer_run_facet):
        import airflow.composer.openlineage.composer_transport
        # Reload the transport module to reevaluate environment variables use in module level constants.
        reload(airflow.composer.openlineage.composer_transport)

        with mock.patch(
            "airflow.composer.openlineage.composer_transport.SyncLineageClient", autospec=True
        ) as mock_sync_lineage_client:
            transport = ComposerTransport(ComposerTransportConfig())
            transport._patch_event(event)

            assert event.job.facets["gcp_composer_job"] == expected_composer_job_facet
            assert event.run.facets["gcp_composer_run"] == expected_composer_run_facet
            assert event.job.facets["gcp_lineage"].origin == GcpOrigin(
                sourceType="COMPOSER",
                name=os.path.join(
                    "projects/test_gcp_project/locations/us-central1/environments/composer-env-name"
                ),
            )
