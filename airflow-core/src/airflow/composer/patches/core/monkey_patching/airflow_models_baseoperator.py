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

from airflow.composer.patches.core.utils import is_triggerer_enabled
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator


def patch():
    BaseOperator.defer = _composer_base_operator_defer(BaseOperator.defer)


def _composer_base_operator_defer(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        if not is_triggerer_enabled():
            raise AirflowException(
                "This Composer environment does not have Airflow triggerer running. "
                "To use deferrable operators enable the triggerer in the environment. "
                "See https://cloud.google.com/composer/docs/composer-3/use-deferrable-operators "
                "for more details."
            )

        return f(self, *args, **kwargs)

    return wrapper
