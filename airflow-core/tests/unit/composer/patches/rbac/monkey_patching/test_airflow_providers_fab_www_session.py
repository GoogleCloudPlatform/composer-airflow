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

from unittest import mock

from airflow.composer.patches.rbac.monkey_patching.airflow_providers_fab_www_session import patch
from airflow.providers.fab.www.session import AirflowDatabaseSessionInterface


class TestAirflowProvidersFabWwwSession:
    @mock.patch(
        "airflow.composer.patches.rbac.monkey_patching.airflow_providers_fab_www_session.AirflowDatabaseSessionInterface.get_expiration_time",
        autospec=True,
    )
    def test_patch_original_value(self, get_expiration_time_mock):
        get_expiration_time_mock.return_value = "original_expiration_time"
        patch()

        actual_expiration_time = AirflowDatabaseSessionInterface.get_expiration_time(
            mock.Mock(), mock.Mock(), {}
        )

        assert actual_expiration_time == "original_expiration_time"

    @mock.patch(
        "airflow.composer.patches.rbac.monkey_patching.airflow_providers_fab_www_session.AirflowDatabaseSessionInterface.get_expiration_time",
        autospec=True,
    )
    def test_patch_overridden_value(self, get_expiration_time_mock):
        get_expiration_time_mock.return_value = "original_expiration_time"
        patch()

        actual_expiration_time = AirflowDatabaseSessionInterface.get_expiration_time(
            mock.Mock(), mock.Mock(), {"_expiration_time": "overridden_expiration_time"}
        )

        assert actual_expiration_time == "overridden_expiration_time"
