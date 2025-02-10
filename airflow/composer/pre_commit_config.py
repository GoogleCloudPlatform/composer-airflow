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
"""Module with method to generate Composer .pre-commit-config.yaml file.

The Composer .pre-commit-config.yaml file is basically copy of the community config file (which is located in
the root of the repo) with some hooks disabled.
"""
import copy
import os
import tempfile

import yaml

HOOKS_TO_DISABLE = [
    "check-tests-in-the-right-folders",
    "insert-license",
    "mypy-airflow",
    # TODO(2.10.5): Reactivate ruff during rebase process so all Composer patches are formatted.
    "ruff",
    "ruff-format",
]


def _create_composer_config(community_config: dict):
    composer_config = copy.deepcopy(community_config)

    for repo in composer_config["repos"]:
        for hook in repo["hooks"]:
            if hook["id"] in HOOKS_TO_DISABLE:
                # Disable hook by making it run only on demand (not automatically).
                hook["stages"] = ["manual"]

    return composer_config


def create_composer_config_file() -> str:
    print("Generating Composer .pre-commit-config.yaml file")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    community_config_file = os.path.abspath(os.path.join(current_dir, "../../.pre-commit-config.yaml"))
    print(f"Using community config file: {community_config_file}")

    with open(community_config_file) as community_config_file_stream:
        community_config = yaml.load(community_config_file_stream, yaml.SafeLoader)

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as composer_config_file_stream:
        composer_config_file = composer_config_file_stream.name
        print(f"Storing Composer .pre-commit-config.yaml file into {composer_config_file}")
        composer_config = _create_composer_config(community_config)
        yaml.dump(composer_config, composer_config_file_stream)

    return composer_config_file
