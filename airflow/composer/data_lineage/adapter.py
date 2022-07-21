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
import datetime
import os
from typing import TYPE_CHECKING, Any, List, Optional

from google.cloud.datacatalog.lineage_v1 import (
    EntityReference,
    LineageEvent,
    LineageEventsBundle,
    Process,
    Run,
)

from airflow.composer.data_lineage.entities import BigQueryTable, DataLineageEntity
from airflow.composer.data_lineage.utils import LOCATION_PATH, get_process_id, get_run_id

if TYPE_CHECKING:
    from airflow.models import TaskInstance
    from airflow.models.baseoperator import BaseOperator

# TODO: extract to composer/utils.py "Composer core patch"
COMPOSER_ENVIRONMENT_NAME = os.environ.get("COMPOSER_ENVIRONMENT")


class ComposerDataLineageAdapter:
    """Adapter for translating Airflow lineage metadata to Data Lineage events."""

    def get_lineage_events_bundle_on_task_completed(
        self,
        task_instance: "TaskInstance",
        inlets: List,
        outlets: List,
    ) -> LineageEventsBundle:
        """Returns Data Lineage events bundle for completed task."""
        process = self._construct_process(task_instance.task)
        run = self._construct_run(task_instance, process.name)
        lineage_events = self._construct_lineage_events(inlets, outlets)

        return LineageEventsBundle(
            process=process,
            run=run,
            lineage_events=lineage_events,
        )

    def _construct_process(self, task: "BaseOperator") -> Process:
        """Returns Process generated based on Airflow task."""
        task_id = task.task_id
        dag_id = task.dag.dag_id

        process_id = get_process_id(
            environment_name=COMPOSER_ENVIRONMENT_NAME, dag_id=dag_id, task_id=task_id
        )
        process_name = os.path.join(LOCATION_PATH, f"processes/{process_id}")

        return Process(
            name=process_name,
            display_name=f"Composer Airflow task {COMPOSER_ENVIRONMENT_NAME}.{dag_id}.{task_id}",
            attributes={
                "composer_environment_name": COMPOSER_ENVIRONMENT_NAME,
                "dag_id": dag_id,
                "task_id": task_id,
                "operator": type(task).__name__,
            },
        )

    def _construct_run(self, task_instance: "TaskInstance", process_name: str) -> Run:
        """Returns Run generated based on Airflow task instance."""
        task_instance_run_id = task_instance.run_id
        run_id = get_run_id(task_instance_run_id)
        run_name = os.path.join(process_name, f"runs/{run_id}")

        return Run(
            name=run_name,
            display_name=f"Airflow task run {task_instance_run_id}",
            attributes={
                "dag_run_id": task_instance_run_id,
            },
            start_time=task_instance.start_date,
            # task_instance.end_date is empty at the moment of sending lineage data.
            end_time=datetime.datetime.utcnow(),
            state="COMPLETED",
        )

    def _construct_lineage_events(self, inlets: List, outlets: List) -> List[LineageEvent]:
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

        return [
            LineageEvent(
                sources=sources,
                targets=targets,
                event_time=datetime.datetime.utcnow(),
            )
        ]

    def _get_entity_reference(self, entity: Any) -> Optional[EntityReference]:
        """Returns Data Lineage entity reference for given Airflow entity (None if entity is unknown)."""
        if isinstance(entity, BigQueryTable):
            return EntityReference(
                fully_qualified_name="bigquery:{}.{}.{}".format(
                    entity.project_id, entity.dataset_id, entity.table_id
                ),
                location="us",  # TODO: provide correct location
            )

        if isinstance(entity, DataLineageEntity):
            return EntityReference(
                fully_qualified_name=entity.fully_qualified_name,
                location=entity.location,
            )

        return None
