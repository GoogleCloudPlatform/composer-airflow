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
import os
from unittest.mock import patch

import yaml

from airflow.composer import pre_commit_config
from airflow.composer.pre_commit_config import (
    _create_composer_config,
    create_composer_config_file
)


class TestPreCommitConfig:
    def test_create_composer_config(self):
        community_config = {
            "repos": [{
                "hooks": [{
                    "id": "hook-1",
                }]
            }, {
                "hooks": [{
                    "id": "hook-2",
                }, {
                    "id": "insert-license",
                }, {
                    "id": "hook-4",
                }]
            }]
        }

        composer_config = _create_composer_config(community_config)

        assert composer_config == {
            "repos": [{
                "hooks": [{
                    "id": "hook-1",
                }]
            }, {
                "hooks": [{
                    "id": "hook-2",
                }, {
                    "id": "insert-license",
                    "stages": ["manual"],
                }, {
                    "id": "hook-4",
                }]
            }]
        }

    def test_create_composer_config_file(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        pre_commit_config_py_file = os.path.join(
            current_dir, "test_data/airflow/composer/pre_commit_config.py")

        with patch.object(pre_commit_config, "__file__", pre_commit_config_py_file):
            composer_config_file = create_composer_config_file()

        with open(composer_config_file) as f:
            actual_content = yaml.load(f.read(), yaml.SafeLoader)
        assert actual_content == {
            "default_stages": ["commit", "push"],
            "minimum_pre_commit_version": "3.2.0",
            "repos": [{
                "repo": "local",
                "hooks": [{
                    "id": "identity",
                    "name": "Print input to the static check hooks for troubleshooting",
                }, {
                    "id": "mypy-airflow",
                    "name": "Run mypy for airflow",
                    "additional_dependencies": ["rich>=12.4.4"],
                    "stages": ["manual"],
                }]
            }]
        }
