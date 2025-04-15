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

from airflow.composer.patches.database_migrations.monkey_patching.airflow_jobs_job import patch
from airflow.jobs.job import Job


class TestAirflowJobsJob:
    @classmethod
    def setup_class(cls):
        patch()

    def test_patch(self):
        assert Job.hostname.type.length == 100

    def test_job_hostname_old_length(self):
        """Test to assure that Job.hostname column length has expected value in community code.

        Once this test fails, we should revisit our Composer patch for this field.
        """
        assert Job.hostname.type._old_length == 500
