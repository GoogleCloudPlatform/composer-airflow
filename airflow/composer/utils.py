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
import sys

import aiodebug.log_slow_callbacks
import requests
from kubernetes import config
from kubernetes.client import Configuration

from airflow.configuration import conf
from airflow.providers.celery.executors.default_celery import DEFAULT_CELERY_CONFIG
from airflow.utils import net

# Enables redis health check in celery. It is set to prevent dags from failing
# when redis closes connection.
COMPOSER_DEFAULT_CELERY_CONFIG = copy.deepcopy(DEFAULT_CELERY_CONFIG)
COMPOSER_DEFAULT_CELERY_CONFIG["redis_backend_health_check_interval"] = 30

COMPOSER_GKE_CLUSTER_HOST = None


def get_composer_version():
    """Return Composer version, e.g. 1.16.5."""
    # FIXME: update Kokoro tests to avoid handling of unknown Composer version here.
    return os.environ.get("COMPOSER_VERSION")


def is_triggerer_enabled():
    enable_triggerer = conf.getboolean("composer_internal", "enable_triggerer", fallback=False)
    return enable_triggerer


def is_composer_v1():
    """Determine if Airflow is running under Composer v1."""
    composer_version = get_composer_version()
    if not composer_version:
        return False

    return composer_version.split(".")[0] == "1"


def is_serverless_composer():
    """Determine if Airflow is running under Composer Serverless (aka Composer 2.50)."""
    composer_version = get_composer_version()
    if not composer_version:
        return False

    major, _, _ = composer_version.split(".", 2)
    major = int(major)
    return major >= 3


def get_component_hostname():
    """
    Act as a custom implementation for airflow.utils.net.get_hostname.

    It makes sure the returned hostname doesn't have ".internal" suffix.
    """
    hostname = net.getfqdn()
    if hostname.endswith(".internal"):
        return hostname[:-9]
    else:
        return hostname


def get_composer_gke_cluster_host():
    global COMPOSER_GKE_CLUSTER_HOST

    if COMPOSER_GKE_CLUSTER_HOST is not None:
        return COMPOSER_GKE_CLUSTER_HOST

    config_file = conf.get("kubernetes_executor", "config_file", fallback=None)
    client_configuration = Configuration()
    config.load_kube_config(
        config_file=config_file, client_configuration=client_configuration, persist_config=False
    )
    COMPOSER_GKE_CLUSTER_HOST = client_configuration.host

    return COMPOSER_GKE_CLUSTER_HOST


def initialize():
    """Act as a hook to do Composer related setup for Airflow."""
    if _is_triggerer_launch_command(sys.argv):
        # This line enables logging slow callbacks in triggers.
        aiodebug.log_slow_callbacks.enable(0.05)

        # TODO: delete when community changes got released https://github.com/apache/airflow/pull/53126
        from airflow.composer.kubernetes.trigger import patch_define_container_state, patch_kubernetes_hook

        patch_kubernetes_hook()
        patch_define_container_state()


def get_locational_endpoint(service, location, version):
    locational_discovery_endpoint = (
        f"https://{location}-{service}.googleapis.com/$discovery/rest?version={version}"
    )
    locational_endpoint = f"{location}-{service}.googleapis.com"
    if is_endpoint_reachable(locational_discovery_endpoint):
        return locational_endpoint


def is_endpoint_reachable(endpoint):
    response = requests.get(endpoint)
    return response.ok


def _is_triggerer_launch_command(cmd_argv: list) -> bool:
    """
    Match triggerer start command.

    Following the pattern: ['/opt/python3.11/bin/airflow', 'triggerer', '--skip-serve-logs'].
    """
    return all(
        [
            "airflow" in cmd_argv[0],
            len(cmd_argv) > 1 and cmd_argv[1] == "triggerer",
        ]
    )
