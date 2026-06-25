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

import attr
from openlineage.client.generated.base import JobFacet, RunFacet
from openlineage.client.utils import RedactMixin


@attr.define
class GcpOrigin(RedactMixin):
    """GCP Origin class."""

    sourceType: str
    name: str


@attr.define
class GcpLineageJobFacet(JobFacet):
    """
    Facet used by Cloud Data Lineage.

    Taken from https://github.com/OpenLineage/OpenLineage/blob/main/spec/registry/gcp/lineage/facets/GcpLineageJobFacet.json
    """

    displayName: str
    origin: GcpOrigin

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/GcpLineageJobFacet.json#/$defs/GcpLineageJobFacet"


@attr.define
class ComposerJobFacet(JobFacet):
    """Job Facet for Composer."""

    environmentName: str
    composerVersion: str
    airflowVersion: str
    dagId: str | None
    taskId: str | None
    operator: str | None


@attr.define
class ComposerRunFacet(RunFacet):
    """Run Facet for Composer."""

    dagRunId: str
