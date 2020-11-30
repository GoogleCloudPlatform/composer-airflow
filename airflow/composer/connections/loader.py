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
"""Module that loads Airflow connection assets required for Composer 1.*.* versions.

For Composer 1.*.* versions we prepare connection assets populated from all available providers.
This is a temporary workaround of not being able to install customer pypi packages to
Airflow UI docker image and therefore extend connection types, details here b/189095667.

See usage in ProvidersManager.
"""
import functools
import os

import dill


@functools.lru_cache()
def _get_connection_assets(assets_id):
    file_path = os.path.join(os.path.dirname(__file__), f"connection_{assets_id}.dill")
    with open(file_path, "rb") as f:
        return dill.load(f)


def get_connection_types():
    """Returns connection types for all providers."""
    return _get_connection_assets("types")


def get_connection_form_widgets():
    """Returns connection form widgets for all providers."""
    return _get_connection_assets("form_widgets")


def get_connection_field_behaviours():
    """Returns connection field behaviours for all providers."""
    return _get_connection_assets("field_behaviours")
