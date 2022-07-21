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

import logging

from airflow.composer.data_lineage.entities import GCSEntity
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

log = logging.getLogger(__name__)


class GCSToGCSOperatorLineageMixin:
    """Mixin class for GCSToGCSOperator."""

    def post_execute_prepare_lineage(self: GCSToGCSOperator, context: dict):  # type: ignore
        source_paths = [self.source_object] if self.source_object else self.source_objects
        inlets = [GCSEntity(bucket=self.source_bucket, path=path) for path in source_paths]

        destination_bucket = self.destination_bucket or self.source_bucket
        if self.destination_object:
            outlets = [GCSEntity(bucket=destination_bucket, path=self.destination_object)]
        else:
            outlets = [GCSEntity(bucket=destination_bucket, path=path) for path in source_paths]

        self.inlets.extend(inlets)
        self.outlets.extend(outlets)
