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

import os
import sys

import aiodebug.log_slow_callbacks
from kubernetes import config
from kubernetes.client import Configuration

from airflow.composer.kubernetes.pod_manager import patch_fetch_container_logs
from airflow.configuration import conf

COMPOSER_GKE_CLUSTER_HOST = None


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
    """Determines if Airflow is running under Composer Serverless (aka Composer 2.5)."""
    composer_version = get_composer_version()
    if not composer_version:
        return False

    major, minor, _ = composer_version.split(".", 2)
    major = int(major)
    minor = int(minor)
    return (major == 2 and minor >= 5) or major > 2


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
    """This method acts as a hook to do Composer related setup for Airflow."""
    if "triggerer" in sys.argv[0]:
        # This line enables logging slow callbacks in triggers.
        aiodebug.log_slow_callbacks.enable(0.05)

    if is_serverless_composer():
        patch_fetch_container_logs()
