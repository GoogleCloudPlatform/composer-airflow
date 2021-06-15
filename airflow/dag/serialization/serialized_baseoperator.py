# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Operator serialization with JSON."""

from airflow.dag.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.dag.serialization.serialization import Serialization
from airflow.models import BaseOperator


class SerializedBaseOperator(BaseOperator, Serialization):
    """A JSON serializable representation of operator.

    All operators are casted to SerializedBaseOperator after deserialization.
    Class specific attributes used by UI are move to object attributes.
    """
    _included_fields = list(
        set(vars(BaseOperator(task_id='test')).keys()) - {
            '_comps', 'inlets', 'outlets', 'lineage_data', 'log', 'resources',
            'on_failure_callback', 'on_success_callback', 'on_retry_callback'
        }) + ['_task_type', 'subdag', 'ui_color', 'ui_fgcolor', 'template_fields']

    def __init__(self, *args, **kwargs):
        BaseOperator.__init__(self, *args, **kwargs)
        # task_type is used by UI to display the correct class type, because UI only
        # receives BaseOperator from deserialized DAGs.
        self._task_type = 'BaseOperator'
        # Move class attributes into object attributes.
        self.ui_color = BaseOperator.ui_color
        self.ui_fgcolor = BaseOperator.ui_fgcolor
        self.template_fields = BaseOperator.template_fields
        # subdag parameter is only set for SubDagOperator.
        # Setting it to None by default as other Operators do not have that field.
        self.subdag = None

    @property
    def task_type(self):
        # Overwrites task_type of BaseOperator to use _task_type instead of
        # __class__.__name__.
        return self._task_type

    @task_type.setter
    def task_type(self, task_type):
        self._task_type = task_type

    @classmethod
    def serialize_operator(cls, op):
        """Serializes operator into a JSON object.
        """
        # FIXME: Note the implementation here is different from the upstream.
        # The upstream loads DAGs from files to display rendered templates.
        # Here it copies all template field attributes into a serialized task.
        serialize_op = cls._serialize_object(op, list(op.template_fields))

        # Adds a new task_type field to record the original operator class.
        serialize_op['_task_type'] = op.__class__.__name__

        if isinstance(op.template_fields, tuple):
            # Don't store the template_fields as a tuple -- a list is simpler and does what we need
            serialize_op['template_fields'] = serialize_op['template_fields'][Encoding.VAR]
        return serialize_op

    @classmethod
    def deserialize_operator(cls, encoded_op):
        """Deserializes an operator from a JSON object.
        """
        op = SerializedBaseOperator(task_id=encoded_op['task_id'])
        cls._deserialize_object(encoded_op, op,
            encoded_op['template_fields'] if 'template_fields' in encoded_op else None)
        return op

    @classmethod
    def _is_excluded(cls, var, attrname, op):
        # For attributes start_date and end_date, if this date is the same as the matching field in the dag,
        # then don't store it again at the task level.
        if attrname.endswith("_date") and var is not None and op.has_dag():
            dag_date = getattr(op.dag, attrname, None)
            if var is dag_date or var == dag_date:
                return True
        return Serialization._is_excluded(var, attrname, op)
