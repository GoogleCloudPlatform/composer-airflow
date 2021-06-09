#
# Copyright 2021 Google LLC
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

import copy
import os

from airflow.configuration import conf
from airflow.providers.celery.executors.default_celery import DEFAULT_CELERY_CONFIG
from airflow.utils import net

# Enables redis health check in celery. It is set to prevent dags from failing
# when redis closes connection.
COMPOSER_DEFAULT_CELERY_CONFIG = copy.deepcopy(DEFAULT_CELERY_CONFIG)
COMPOSER_DEFAULT_CELERY_CONFIG["redis_backend_health_check_interval"] = 30


def get_composer_version():
    """Returns Composer version, e.g. 1.16.5."""
    # FIXME: update Kokoro tests to avoid handling of unknown Composer version here.
    return os.environ.get("COMPOSER_VERSION")


def is_triggerer_enabled():
    enable_triggerer = conf.getboolean("composer_internal", "enable_triggerer", fallback=False)
    return enable_triggerer


def is_composer_v1():
    """Determines if Airflow is running under Composer v1."""
    composer_version = get_composer_version()
    if not composer_version:
        return False

    return composer_version.split(".")[0] == "1"


def is_serverless_composer():
    """Determines if Airflow is running under Composer Serverless (aka Composer 2.50)."""
    composer_version = get_composer_version()
    if not composer_version:
        return False

    major, _, _ = composer_version.split(".", 2)
    major = int(major)
    return major >= 3


def get_component_hostname():
    """Custom implementation for airflow.utils.net.get_hostname.

    It makes sure the returned hostname doesn't have ".internal" suffix.
    """
    hostname = net.getfqdn()
    if hostname.endswith(".internal"):
        return hostname[:-9]
    else:
        return hostname


def initialize():
    """This method acts as a hook to do Composer related setup for Airflow."""
