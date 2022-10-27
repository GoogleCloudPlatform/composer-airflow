#
# Copyright 2020 Google LLC
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
"""Composer Data Lineage changes in Airflow operators.

Code implemented in this module and its submodules are supposed to be moved to actual operator
code base. As of now it resides here as Composer patch to Airflow until it will be contributed to
community code base.

The structure of submodules should repeat structure of "airflow.providers" package.
Each operator should have one mixin class that implements a method "post_execute_prepare_lineage"
which will be called after task execution in BaseOperator.post_execute() method.

If operator requires a new Airflow lineage entity, it should be defined in
"airflow.composer.data_lineage.entities" module.
"""
from typing import TYPE_CHECKING, Dict

from airflow.composer.data_lineage.operators.google.cloud.bigquery import (
    BigQueryInsertJobOperatorLineageMixin,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator

_OPERATOR_TO_MIXIN = {
    BigQueryInsertJobOperator: BigQueryInsertJobOperatorLineageMixin,
}


def post_execute_prepare_lineage(task: "BaseOperator", context: Dict):
    """Prepares the lineage inlets and outlets for a given task."""
    operator = type(task)
    if operator not in _OPERATOR_TO_MIXIN:
        return

    mixin = _OPERATOR_TO_MIXIN[operator]
    mixin.post_execute_prepare_lineage(task, context)
