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

from airflow import plugins_manager
from airflow.composer.patches.metrics.plugin import register_composer_metrics_plugin


def patch():
    plugins_manager.ensure_plugins_loaded = _composer_plugins_manager_ensure_plugins_loaded(
        plugins_manager.ensure_plugins_loaded
    )


def _composer_plugins_manager_ensure_plugins_loaded(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        res = f(*args, **kwargs)

        register_composer_metrics_plugin()

        return res

    return wrapper
