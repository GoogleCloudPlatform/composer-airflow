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

import yaml

HOOKS_TO_DISABLE = [
    "check-tests-in-the-right-folders",
    "insert-license",
    "mypy-airflow",
    # TODO(2.10.5): Reactivate ruff during rebase process so all Composer patches are formatted.
    "ruff",
    "ruff-format",
]


HOOK_TO_EXCLUSION_MAP = {"check-cncf-k8s-only-for-executors": "^airflow/composer/"}


def _create_composer_config(community_config: dict):
    composer_config = copy.deepcopy(community_config)

    for repo in composer_config["repos"]:
        for hook in repo["hooks"]:
            hook_id = hook["id"]
            if hook_id in HOOKS_TO_DISABLE:
                # Disable hook by making it run only on demand (not automatically).
                hook["stages"] = ["manual"]
            if hook_id in HOOK_TO_EXCLUSION_MAP:
                if hook.get("exclude"):
                    hook["exclude"] += "|" + HOOK_TO_EXCLUSION_MAP[hook_id]
                else:
                    hook["exclude"] = HOOK_TO_EXCLUSION_MAP[hook_id]

    return composer_config


def create_composer_config_file() -> str:
    """Creates Composer .pre-commit-config.yaml file in .git/hooks/ folder."""
    print("Generating Composer .pre-commit-config.yaml file")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    community_config_file = os.path.abspath(os.path.join(current_dir, "../../.pre-commit-config.yaml"))
    composer_config_file = os.path.abspath(
        os.path.join(current_dir, "../../.git/hooks/.composer-pre-commit-config.yaml"))
    print(f"Using community config file: {community_config_file}")

    with open(community_config_file) as f:
        community_config = yaml.load(f, yaml.SafeLoader)

    with open(composer_config_file, "w") as f:
        print(f"Storing Composer .pre-commit-config.yaml file into {composer_config_file}")
        composer_config = _create_composer_config(community_config)
        yaml.dump(composer_config, f)


if __name__ == "__main__":
    create_composer_config_file()
