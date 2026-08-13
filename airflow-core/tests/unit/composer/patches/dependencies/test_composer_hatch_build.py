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

from collections import defaultdict
from unittest import mock

from airflow.composer.patches.dependencies.composer_hatch_build import build_hook_initialize


class TestComposerHatchBuild:
    def test_build_hook_initialize_extends_with_composer_dependencies(self):
        optional_dependencies = defaultdict(list)
        optional_dependencies["statsd"] = ["statsd-dep"]
        build_hook = mock.MagicMock(
            metadata=mock.MagicMock(
                core=mock.MagicMock(
                    _optional_dependencies=optional_dependencies,
                )
            )
        )
        build_data = {
            "dependencies": ["dep-1", "dep-2"],
        }

        build_hook_initialize(build_hook, build_data)

        # Check that existing dependencies are still there.
        assert "dep-1" in build_data["dependencies"]
        assert "dep-2" in build_data["dependencies"]
        # Check that Composer dependencies are added.
        assert "apache-airflow-providers-ssh" in build_data["dependencies"]
        # Check that non pypi.org Composer dependencies are added.
        assert "google-cloud-datacatalog-lineage-producer-client" in build_data["dependencies"]
        # Check that Composer Airflow extras dependencies are added.
        assert "statsd-dep" in build_data["dependencies"]
