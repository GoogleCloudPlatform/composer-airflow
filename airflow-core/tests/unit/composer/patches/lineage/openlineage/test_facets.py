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

from airflow.composer.patches.lineage.openlineage.facets import GcpLineageJobFacet, GcpOrigin


class TestFacets:
    def test_GcpLineageJobFacet_get_schema(self):
        gcp_lineage_facet = GcpLineageJobFacet(
            displayName=("Composer Airflow Job test-environment.job-name"),
            origin=GcpOrigin(
                sourceType="COMPOSER",
                name="projects/project-id/locations/test-location/environments/test-environment",
            ),
        )

        assert (
            gcp_lineage_facet._get_schema()
            == "https://openlineage.io/spec/facets/1-0-0/GcpLineageJobFacet.json#/$defs/GcpLineageJobFacet"
        )
