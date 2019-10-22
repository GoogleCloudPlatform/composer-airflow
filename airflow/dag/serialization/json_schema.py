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

"""jsonschema for validating serialized DAG and operator."""

import json
import pkgutil

import jsonschema

from airflow.exceptions import AirflowException


def load_dag_schema():
    """Load Json Schema for DAG.

    :return: a type validator with type jsonschema.Draft7Validator
    """
    schema_file_name = 'schema.json'
    schema_file = pkgutil.get_data(__name__, schema_file_name)

    if schema_file is None:
        raise AirflowException("Schema file {} does not exists".format(schema_file_name))

    schema = json.loads(schema_file.decode())
    jsonschema.Draft7Validator.check_schema(schema)
    return jsonschema.Draft7Validator(schema)
