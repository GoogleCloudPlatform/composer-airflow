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

import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import yaml

from airflow.composer.patches.dependencies import pre_commit_config
from airflow.composer.patches.dependencies.pre_commit_config import (
    _create_composer_config,
    create_composer_config_file,
)


class TestPreCommitConfig:
    def test_create_composer_config(self):
        community_config = {
            "repos": [
                {
                    "hooks": [
                        {
                            "id": "hook-1",
                        }
                    ]
                },
                {
                    "hooks": [
                        {
                            "id": "hook-2",
                        },
                        {
                            "id": "insert-license",
                        },
                        {
                            "id": "hook-4",
                        },
                    ]
                },
            ]
        }

        composer_config = _create_composer_config(community_config)

        assert composer_config == {
            "repos": [
                {
                    "hooks": [
                        {
                            "id": "hook-1",
                        }
                    ]
                },
                {
                    "hooks": [
                        {
                            "id": "hook-2",
                        },
                        {
                            "id": "insert-license",
                            "stages": ["manual"],
                        },
                        {
                            "id": "hook-4",
                        },
                    ]
                },
            ]
        }

    def test_create_composer_config_file(self):
        tmp_dir = tempfile.mkdtemp()
        current_dir = os.path.dirname(os.path.abspath(__file__))
        shutil.copy(os.path.join(current_dir, "test_data/.pre-commit-config.yaml"), tmp_dir)
        Path(os.path.join(tmp_dir, ".git/hooks/")).mkdir(parents=True, exist_ok=True)

        with patch.object(
            pre_commit_config,
            "__file__",
            os.path.join(
                tmp_dir, "airflow-core/src/airflow/composer/patches/dependencies/pre_commit_config.py"
            ),
        ):
            create_composer_config_file()

        with open(os.path.join(tmp_dir, ".git/hooks/.composer-pre-commit-config.yaml")) as f:
            actual_content = yaml.load(f.read(), yaml.SafeLoader)
            assert actual_content == {
                "default_stages": ["commit", "push"],
                "minimum_pre_commit_version": "3.2.0",
                "repos": [
                    {
                        "repo": "local",
                        "hooks": [
                            {
                                "id": "identity",
                                "name": "Print input to the static check hooks for troubleshooting",
                            },
                            {
                                "id": "mypy-airflow",
                                "name": "Run mypy for airflow",
                                "additional_dependencies": ["rich>=12.4.4"],
                                "stages": ["manual"],
                            },
                        ],
                    }
                ],
            }

        shutil.rmtree(tmp_dir)
