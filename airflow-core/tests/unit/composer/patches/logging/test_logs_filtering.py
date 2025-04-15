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

import subprocess


class TestLogsFiltering:
    def test_filter_warnings_using_the_in_memory_storage(self):
        message = (
            "previous text/ Using the in-memory storage for tracking rate limits "
            "as no storage was explicitly specified. other text..."
        )

        output = subprocess.check_output(
            [
                "python",
                "-c",
                (
                    "import warnings; "
                    "from airflow.composer.patches.logging.logs_filtering import filter_warnings; "
                    "filter_warnings(); "
                    # We should use here double quotes around message (as it is used above) to have proper
                    # python string formatting.
                    f'warnings.warn("{message}")'
                ),
            ],
            stderr=subprocess.STDOUT,
        ).decode()

        assert message not in output
