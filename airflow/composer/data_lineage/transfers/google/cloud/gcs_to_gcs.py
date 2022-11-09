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
import logging
from typing import Dict

from airflow.composer.data_lineage.entities import GCSEntity
from airflow.providers.google.cloud.transfers.gcs_to_gcs import WILDCARD

log = logging.getLogger(__name__)


class GCSToGCSOperatorLineageMixin:
    """Mixin class for GCSToGCSOperator."""

    def post_execute_prepare_lineage(self, context: Dict):
        _source_paths = [self.source_object] if self.source_object else self.source_objects
        inlet_paths = []
        for _source_path in _source_paths:
            if WILDCARD in _source_path:
                # We report object's folder if wildcard was used, for example:
                # 'sales/sales-2017/prefix_*.avro' => 'sales/sales-2017/'
                _source_path = _source_path.split(WILDCARD)[0]
                _source_path = _source_path.rsplit('/', 1)[0] + '/'
            inlet_paths.append(_source_path)
        inlets = [GCSEntity(bucket=self.source_bucket, path=path) for path in inlet_paths]

        destination_bucket = self.destination_bucket or self.source_bucket
        if self.destination_object:
            # TODO: we should here cover cases, described in the bug b/258635213 immediately after getting
            #  it fixed or GCSToGCSOperator refactored
            outlets = [GCSEntity(bucket=destination_bucket, path=self.destination_object)]
        else:
            outlets = [GCSEntity(bucket=destination_bucket, path=path) for path in inlet_paths]

        self.inlets.extend(inlets)
        self.outlets.extend(outlets)
