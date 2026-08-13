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

from datetime import datetime, timedelta

from airflow.composer.patches.database_retention.tables import tables_to_trim
from airflow.sdk._shared.timezones.timezone import utc


class Config:
    """Internal data structure for database retention configuration."""

    def __init__(self, retention_days):
        self.retention_days = retention_days

        execution_time = datetime.now(tz=utc)
        self.execution_time_str = execution_time.strftime("'%Y-%m-%d %H:%M:%S'")
        self.expiration_datetime = execution_time - timedelta(days=self.retention_days)

        self.tables = tables_to_trim()
