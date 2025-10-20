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

import uvicorn

from airflow.composer.patches.core.monkey_patching.uvicorn import patch


class TestUvicorn:
    @mock.patch("uvicorn.run", autospec=True)
    def test_patch_uvicorn_run(self, run_mock):
        patch()

        uvicorn.run("app", host="123.40.1.2", port=1234)

        run_mock.assert_called_once_with("app", host="123.40.1.2", port=1234, timeout_worker_healthcheck=60)
