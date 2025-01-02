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
from airflow.composer.openlineage.utils import sanitize_display_name


class TestUtils:
    def test_sanitize_display_name(self):
        actual_sanitized_display_name = sanitize_display_name(
            "Composer Airflow task dag_id.task+17*_0-9 :&" + ("X" * 300)
        )

        expected_sanitized_display_name = "Composer Airflow task dag_id.task17_0-9 :&" + ("X" * 158)
        assert actual_sanitized_display_name == expected_sanitized_display_name
