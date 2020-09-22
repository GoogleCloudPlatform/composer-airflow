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

from functools import wraps
from typing import Callable, Optional, Sequence, Tuple, TypeVar, cast

from flask import Response, current_app, g

from airflow.api_connexion.exceptions import PermissionDenied, Unauthenticated

T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def check_authentication() -> None:
    """Checks that the request has valid authorization information."""
    response = current_app.api_auth.requires_authentication(Response)()
    if response.status_code != 200:
        # since this handler only checks authentication, not authorization,
        # we should always return 401
        raise Unauthenticated(headers=response.headers)


def can_access_any_dags(action: str, dag_id: Optional[int] = None) -> bool:
    """Checks if user has read or write access to some dags."""
    appbuilder = current_app.appbuilder
    if dag_id and dag_id != '~':
        return appbuilder.sm.has_access(action, dag_id)

    user = g.user
    if action == 'can_read':
        return any(appbuilder.sm.get_readable_dags(user))
    return any(appbuilder.sm.get_editable_dags(user))


def check_authorization(
    permissions: Optional[Sequence[Tuple[str, str]]] = None, dag_id: Optional[int] = None
) -> None:
    """Checks that the logged in user has the specified permissions."""
    if not permissions:
        return
    appbuilder = current_app.appbuilder
    for permission in permissions:
        if permission in (('can_read', 'Dag'), ('can_edit', 'Dag')):
            can_access_all_dags = appbuilder.sm.has_access(*permission)
            if can_access_all_dags:
                continue

            action = permission[0]
            if can_access_any_dags(action, dag_id):
                continue

            raise PermissionDenied()

        elif not appbuilder.sm.has_access(*permission):
            raise PermissionDenied()


def requires_access(permissions: Optional[Sequence[Tuple[str, str]]] = None) -> Callable[[T], T]:
    """
    Factory for decorator that checks current user's permissions against required permissions.
    """
    appbuilder = current_app.appbuilder
    appbuilder.sm.sync_resource_permissions(permissions)

    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):

            check_authentication()
            check_authorization(permissions, kwargs.get('dag_id', None))

            return func(*args, **kwargs)

        return cast(T, decorated)

    return requires_access_decorator
