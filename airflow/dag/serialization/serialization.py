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

"""Utils for DAG serialization with JSON."""

import datetime
import enum
import json
import logging
import time

import jsonschema
import pendulum
import six

import airflow
from airflow.dag.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, DAG
from airflow.models import Connection
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.timezone import utc_epoch
from airflow.www.utils import get_python_source


LOG = LoggingMixin().log

# Serialization failure returns 'failed'.
FAILED = 'serialization_failed'


def safe_str(var):
    if six.PY2 and isinstance(var, unicode):
        return var
    return str(var)


UTC_EPOCH = utc_epoch()

def datetime_to_timestamp(var):
    if six.PY2:
        return (var - UTC_EPOCH).total_seconds()
    return var.timestamp()


class Serialization:
    """Serialization provides utils for serialization."""

    # JSON primitive types. unicode is included in six.string_types.
    _primitive_types = (int, bool, float, six.string_types)

    # Time types.
    # Airflow requires timestamps to be datetime.datetime.
    # datetime.date and datetime.time are dumped to strings.
    _datetime_types = (datetime.datetime,)

    # Exactly these fields will be contained in the serialized Json
    _included_fields = []

    # Object types that are always excluded in serialization.
    # FIXME: not needed if _included_fields of DAG and operator are customized.
    _excluded_types = (logging.Logger, Connection, type)

    _json_schema = None

    @classmethod
    def to_json(cls, var):
        """Stringifies DAGs and operators contained by var and returns a JSON string of var."""
        return json.dumps(cls._serialize(var), ensure_ascii=True)

    @classmethod
    def to_dict(cls, var):
        """Stringifies DAGs and operators contained by var and returns a dict of var."""
        return cls._serialize(var)

    @classmethod
    def from_json(cls, serialized_obj):
        """Deserializes serialized_obj and reconstructs all DAGs and operators it contains."""
        return cls._deserialize(json.loads(serialized_obj))

    @classmethod
    def from_dict(cls, serialized_obj):
        """Deserializes serialized_obj and reconstructs all DAGs and operators it contains."""
        return cls._deserialize(serialized_obj)

    @classmethod
    def validate_schema(cls, serialized_obj):
        """Validate serialized_obj satisfies JSON schema."""
        if cls._json_schema is None:
            raise AirflowException('JSON schema of {:s} is not set.'.format(cls.__name__))

        if isinstance(serialized_obj, str):
            serialized_obj = json.loads(serialized_obj)
        cls._json_schema.validate(serialized_obj)

    @staticmethod
    def _encode(var, type_):
        """Encode data by a JSON dict."""
        return {Encoding.VAR: var, Encoding.TYPE: type_}

    @classmethod
    def _serialize_object(cls, var, additional_fields=None):
        """Helper function to serialize an object as a JSON dict."""
        fields = cls._included_fields
        if additional_fields:
            fields += additional_fields

        new_var = {}
        for k in fields:
            # None is ignored in serialized form and is added back in deserialization.
            v = getattr(var, k, None)
            if not cls._is_excluded(v, k, var):
                new_var[k] = cls._serialize(v)
        return new_var

    @classmethod
    def _deserialize_object(cls, var, new_var, additional_fields=None):
        """Deserialize and copy the attributes of dict var to a new object new_var."""
        fields = cls._included_fields
        if additional_fields:
            fields += additional_fields

        for k in fields:
            if k in var:
                setattr(new_var, k, cls._deserialize(var[k]))
            else:
                setattr(new_var, k, None)

    @classmethod
    def _is_primitive(cls, var):
        """Primitive types."""
        return var is None or (isinstance(var, cls._primitive_types) and
                               not isinstance(var, enum.Enum))

    @classmethod
    def _is_excluded(cls, var, attrname, instance):
        """Types excluded from serialization."""
        # pylint: disable=unused-argument
        return var is None or isinstance(var, cls._excluded_types)

    @classmethod
    def _serialize(cls, var):  # pylint: disable=too-many-return-statements
        """Helper function of depth first search for serialization.

        visited_dags stores DAGs that are being serialized or have been serialized,
        for:
        (1) preventing deadlock loop caused by task.dag, task._dag, and dag.parent_dag;
        (2) replacing the fields in (1) with serialized counterparts.

        The serialization protocol is:
        (1) keeping JSON supported types: primitives, dict, list;
        (2) encoding other types as {TYPE: 'foo', VAR: 'bar'}, the deserialization
            step decode VAR according to TYPE;
        (3) Operator has a special field CLASS to record the original class
            name for displaying in UI.
        """
        try:
            if cls._is_primitive(var):
                return var
            elif isinstance(var, dict):
                return cls._encode(
                    {safe_str(k): cls._serialize(v) for k, v in var.items()},
                    type_=DAT.DICT)
            elif isinstance(var, list):
                return [cls._serialize(v) for v in var]
            elif isinstance(var, DAG):
                return cls._encode(
                    airflow.dag.serialization.SerializedDAG.serialize_dag(var),
                    type_=DAT.DAG)
            elif isinstance(var, BaseOperator):
                return cls._encode(
                    airflow.dag.serialization.SerializedBaseOperator.serialize_operator(var),
                    type_=DAT.OP)
            elif isinstance(var, cls._datetime_types):
                return cls._encode(datetime_to_timestamp(var), type_=DAT.DATETIME)
            elif isinstance(var, datetime.timedelta):
                return cls._encode(var.total_seconds(), type_=DAT.TIMEDELTA)
            elif isinstance(var, (pendulum.tz.Timezone, pendulum.tz.timezone_info.TimezoneInfo)):
                return cls._encode(safe_str(var.name), type_=DAT.TIMEZONE)
            elif callable(var):
                return safe_str(get_python_source(var))
            elif isinstance(var, set):
                # FIXME: casts set to list in customized serialization in future.
                return cls._encode(
                    [cls._serialize(v) for v in var], type_=DAT.SET)
            elif isinstance(var, tuple):
                # FIXME: casts tuple to list in customized serialization in future.
                return cls._encode(
                    [cls._serialize(v) for v in var], type_=DAT.TUPLE)
            else:
                LOG.debug('Cast type %s to str in serialization.', type(var))
                return safe_str(var)
        except Exception:  # pylint: disable=broad-except
            LOG.warning('Failed to serialize.', exc_info=True)
            return FAILED

    @classmethod
    def _deserialize(cls, encoded_var):  # pylint: disable=too-many-return-statements
        """Helper function of depth first search for deserialization."""
        try:
            # JSON primitives (except for dict) are not encoded.
            if cls._is_primitive(encoded_var):
                return encoded_var
            elif isinstance(encoded_var, list):
                return [cls._deserialize(v) for v in encoded_var]

            assert isinstance(encoded_var, dict)
            var = encoded_var[Encoding.VAR]
            type_ = encoded_var[Encoding.TYPE]

            if type_ == DAT.DICT:
                return {k: cls._deserialize(v) for k, v in var.items()}
            elif type_ == DAT.DAG:
                return airflow.dag.serialization.SerializedDAG.deserialize_dag(var)
            elif type_ == DAT.OP:
                return airflow.dag.serialization.SerializedBaseOperator.deserialize_operator(var)
            elif type_ == DAT.DATETIME:
                return pendulum.from_timestamp(var)
            elif type_ == DAT.TIMEDELTA:
                return datetime.timedelta(seconds=var)
            elif type_ == DAT.TIMEZONE:
                return pendulum.timezone(var)
            elif type_ == DAT.SET:
                return {cls._deserialize(v) for v in var}
            elif type_ == DAT.TUPLE:
                return tuple([cls._deserialize(v) for v in var])
            else:
                LOG.warning('Invalid type %s in deserialization.', type_)
                return None
        except Exception:  # pylint: disable=broad-except
            LOG.warning('Failed to deserialize %s.', encoded_var, exc_info=True)
            return None
