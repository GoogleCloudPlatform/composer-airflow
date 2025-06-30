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

import functools
import inspect
import logging
import os
import pkgutil
import sys
from importlib import import_module

from kubernetes import config
from kubernetes.client import Configuration

from airflow.configuration import conf
from airflow.utils import net

logger = logging.getLogger(__name__)

COMPOSER_GKE_CLUSTER_HOST = None

COMPOSER_PATCHES_PACKAGE = "airflow.composer.patches"
# /.../airflow/composer/patches/
COMPOSER_PATCHES_PACKAGE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")


def is_triggerer_enabled():
    enable_triggerer = conf.getboolean("composer_internal", "enable_triggerer", fallback=False)
    return enable_triggerer


def get_component_hostname():
    """
    Return hostname of the component.

    This method is custom implementation for airflow.utils.net.get_hostname, it makes sure the returned
    hostname doesn't have ".internal" suffix.
    """
    hostname = net.getfqdn()
    if hostname.endswith(".internal"):
        return hostname[:-9]
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


def is_currently_running_component(component_name):
    """
    Check if currently running Airflow component is the one provided in the argument.

    Example:
        if is_currently_running_component("triggerer"):
            ...
    """
    return len(sys.argv) >= 2 and sys.argv[1] == component_name


def cross_composer_patches_method(f):
    """
    Execute same method across all Composer patches.

    This decorator should be used for methods like `initialize` that have implementation spread across
    multiple Composer patches.

    Usage example, in airflow/composer/patches/core/airflow_local_settings.py:
        @cross_composer_patches_method
        def initialize():
            ...
    In this example, `cross_composer_patches_method` decorator will make sure that all `initialize` methods
    located in airflow_local_settings.py files of the root of other patches are executed.

    Refer to the implementation for details.
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        f_name = f.__name__
        f_module_name = inspect.getmodule(f).__name__.rsplit(".", 1)[-1]

        logger.debug("Composer core %s", f_name)
        f(*args, **kwargs)

        patch_names = [m_info.name for m_info in pkgutil.iter_modules([COMPOSER_PATCHES_PACKAGE_PATH])]
        # Iterate over sorted list of patch names to make code deterministic.
        for patch_name in sorted(patch_names):
            # Skip "core" patch, its method called already above.
            if patch_name == "core":
                continue

            # Get patch module with the same name as the module name of `f`. Skip if not found.
            try:
                patch_module = import_module(f"{COMPOSER_PATCHES_PACKAGE}.{patch_name}.{f_module_name}")
            except ModuleNotFoundError:
                continue

            # Get patch method with the same name as the name of `f`. Skip if not found.
            try:
                patch_method = getattr(patch_module, f_name)
            except AttributeError:
                continue

            # Execute patch method.
            logger.debug("Composer %s %s", patch_name, f_name)
            patch_method(*args, **kwargs)

    return wrapper


def apply_monkey_patching_patches():
    """
    Apply all patches from monkey_patching/ folder of each Composer patch.

    Effectively, this method looks for modules inside monkey_patching/ folder of each Composer patch package
    and executes patch() method of each such found module.

    The patch() methods are supposed to perform patching of respective modules.
    """
    patch_names = [m_info.name for m_info in pkgutil.iter_modules([COMPOSER_PATCHES_PACKAGE_PATH])]

    # Iterate over sorted list of patch names to make code execution deterministic.
    for patch_name in sorted(patch_names):
        for module_to_patch_info in pkgutil.iter_modules(
            [os.path.join(COMPOSER_PATCHES_PACKAGE_PATH, patch_name, "monkey_patching")]
        ):
            module_to_patch = import_module(
                f"{COMPOSER_PATCHES_PACKAGE}.{patch_name}.monkey_patching.{module_to_patch_info.name}"
            )

            # Patch module.
            module_to_patch.patch()
