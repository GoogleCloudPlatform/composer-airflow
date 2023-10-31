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
"""Composer Data Lineage adapter implementation."""
from __future__ import annotations

import datetime
import os
from typing import TYPE_CHECKING, Any

import re2
from google.cloud.datacatalog.lineage_v1 import EntityReference, EventLink, LineageEvent, Origin, Process, Run

from airflow.composer.data_lineage.entities import (
    BigQueryTable,
    DataLineageEntity,
    DataprocMetastoreTable,
    GCSEntity,
    MySQLTable,
    PostgresTable,
)
from airflow.composer.data_lineage.utils import LOCATION_PATH, get_process_id, get_run_id

if TYPE_CHECKING:
    from airflow.models import TaskInstance
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.mappedoperator import MappedOperator

# TODO: extract to composer/utils.py "Composer core patch"
COMPOSER_ENVIRONMENT_NAME = os.environ.get("COMPOSER_ENVIRONMENT")


class ComposerDataLineageAdapter:
    """Adapter for translating Airflow lineage metadata to Data Lineage events."""

    def get_lineage_events_bundle_on_task_completed(
        self,
        task_instance: TaskInstance,
        inlets: list,
        outlets: list,
    ) -> dict[str, Any]:
        """Returns Data Lineage events bundle for completed task."""
        process = self._construct_process(task_instance.task)
        run = self._construct_run(task_instance, process.name)
        lineage_events = self._construct_lineage_events(inlets, outlets)

        return dict(
            process=process,
            run=run,
            lineage_events=lineage_events,
        )

    def _construct_process(self, task: BaseOperator | MappedOperator) -> Process:
        """Returns Process generated based on Airflow task."""
        task_id = task.task_id
        dag_id = task.dag.dag_id  # type: ignore

        process_id = get_process_id(
            environment_name=COMPOSER_ENVIRONMENT_NAME, dag_id=dag_id, task_id=task_id  # type: ignore
        )
        process_name = os.path.join(LOCATION_PATH, f"processes/{process_id}")
        origin_name = os.path.join(LOCATION_PATH, f"environments/{COMPOSER_ENVIRONMENT_NAME}")

        return Process(
            name=process_name,
            display_name=self._sanitize_display_name(
                f"Composer Airflow task {COMPOSER_ENVIRONMENT_NAME}.{dag_id}.{task_id}"
            ),
            attributes={
                "composer_environment_name": COMPOSER_ENVIRONMENT_NAME,
                "dag_id": dag_id,
                "task_id": task_id,
                "operator": type(task).__name__,
            },
            origin=Origin(
                source_type=Origin.SourceType.COMPOSER,
                name=origin_name,
            ),
        )

    def _construct_run(self, task_instance: TaskInstance, process_name: str) -> Run:
        """Returns Run generated based on Airflow task instance."""
        task_instance_run_id = task_instance.run_id
        run_id = get_run_id(task_instance_run_id)
        run_name = os.path.join(process_name, f"runs/{run_id}")

        return Run(
            name=run_name,
            display_name=self._sanitize_display_name(f"Airflow task run {task_instance_run_id}"),
            attributes={
                "dag_run_id": task_instance_run_id,
            },
            start_time=task_instance.start_date,
            # task_instance.end_date is empty at the moment of sending lineage data.
            end_time=datetime.datetime.utcnow(),
            state="COMPLETED",
        )

    def _construct_lineage_events(self, inlets: list, outlets: list) -> list[LineageEvent]:
        """Returns Data Lineage events generated based on Airflow inlets/outlets.

        Note: if one of the given inlets/outlets is unknown (_get_entity_reference returned None),
            then this method returns empty list.
        """
        if not inlets and not outlets:
            return []

        sources = []
        for inlet in inlets:
            entity_reference = self._get_entity_reference(inlet)
            if entity_reference is None:
                return []

            sources.append(entity_reference)

        targets = []
        for outlet in outlets:
            entity_reference = self._get_entity_reference(outlet)
            if entity_reference is None:
                return []

            targets.append(entity_reference)

        now = datetime.datetime.utcnow()
        all_links = [EventLink(source=s, target=t) for s in sources for t in targets]
        links_batches = [all_links[i : i + 100] for i in range(0, len(all_links), 100)]
        return [
            LineageEvent(
                links=links,
                start_time=now,
                end_time=now,
            )
            for links in links_batches
        ]

    def _get_fqn_suffix(self, segments: list[str]) -> str:
        """Escapes segments if necessary and concatenates them with '.'."""

        def encode_fqn_path_segment(segment: str) -> str:
            reserved_characters = [" ", "\n", "\t", ".", ":", "`"]
            to_escape = any(c in segment for c in reserved_characters)
            output = segment.replace("`", "``")
            if to_escape:
                output = "`" + output + "`"
            return output

        return ".".join(map(encode_fqn_path_segment, segments))

    def _get_entity_reference(self, entity: Any) -> EntityReference | None:
        """Returns Data Lineage entity reference for given Airflow entity (None if entity is unknown).

        ref: https://cloud.google.com/data-catalog/docs/fully-qualified-names
        """
        if isinstance(entity, BigQueryTable):
            suffix = self._get_fqn_suffix([entity.project_id, entity.dataset_id, entity.table_id])
            return EntityReference(fully_qualified_name=f"bigquery:{suffix}")

        if isinstance(entity, DataLineageEntity):
            return EntityReference(fully_qualified_name=entity.fully_qualified_name)

        if isinstance(entity, GCSEntity):
            suffix = self._get_fqn_suffix([entity.bucket, entity.path])
            return EntityReference(fully_qualified_name=f"gcs:{suffix}")

        if isinstance(entity, MySQLTable):
            suffix = self._get_fqn_suffix([f"{entity.host}:{entity.port}", entity.schema, entity.table])
            return EntityReference(fully_qualified_name=f"mysql:{suffix}")

        if isinstance(entity, PostgresTable):
            suffix = self._get_fqn_suffix(
                [f"{entity.host}:{entity.port}", entity.database, entity.schema, entity.table]
            )
            return EntityReference(fully_qualified_name=f"postgresql:{suffix}")

        if isinstance(entity, DataprocMetastoreTable):
            suffix = self._get_fqn_suffix(
                [entity.project_id, entity.location, entity.instance_id, entity.database, entity.table]
            )
            return EntityReference(fully_qualified_name=f"dataproc_metastore:{suffix}")

        return None

    def _sanitize_display_name(self, display_name: str) -> str:
        """Sanitizes display_name for Process and Run.

        See Data Lineage API spec for supported characters.
        """
        return re2.sub(r"[^A-Za-z0-9 _\-:&.]", "", display_name)[:200]