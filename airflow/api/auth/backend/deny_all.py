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
"""Authentication backend that denies all requests"""
from functools import wraps
from typing import Optional

from flask import Response

from airflow.api.auth.backend.default import ClientAuthProtocol

CLIENT_AUTH = None  # type: Optional[ClientAuthProtocol]


def init_app(_):
    """Initializes authentication"""


def requires_authentication(function):
    """Decorator for functions that require authentication"""

    # noinspection PyUnusedLocal
    @wraps(function)
    def decorated(*args, **kwargs):  # pylint: disable=unused-argument
        return Response("Forbidden", 403)

    return decorated
