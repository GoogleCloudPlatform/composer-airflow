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
"""Composer authentication backend"""
from functools import wraps
from typing import Any, Callable, Optional, Tuple, TypeVar, Union, cast

from flask import Response, current_app

from airflow.configuration import conf

CLIENT_AUTH: Optional[Union[Tuple[str, str], Any]] = None

# The default user self registration role.
COMPOSER_AUTH_USER_REGISTRATION_ROLE = conf.get(
    "api", "composer_auth_user_registration_role", fallback="Public"
)


def init_app(_):
    """Initializes authentication backend"""


T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def requires_authentication(function: T):
    """Decorator for functions that require authentication"""

    @wraps(function)
    def decorated(*args, **kwargs):
        if (
            current_app.appbuilder.sm.auth_view.auth_current_user(
                user_registration_role=COMPOSER_AUTH_USER_REGISTRATION_ROLE
            )
            is not None
        ):
            return function(*args, **kwargs)
        else:
            return Response("Unauthorized", 401, {"WWW-Authenticate": "Composer"})

    return cast(T, decorated)
