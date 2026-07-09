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
from __future__ import annotations

from typing import TYPE_CHECKING

from flask import g
from sqlalchemy import func, select
from sqlalchemy.orm import object_session

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound, PermissionDenied
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.error_schema import (
    ImportErrorCollection,
    import_error_collection_schema,
    import_error_schema,
)
from airflow.auth.managers.models.resource_details import AccessView
from airflow.configuration import conf
from airflow.models.dag import DagModel
from airflow.models.errors import ParseImportError
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse


REDACTED_IMPORT_ERROR_STACKTRACE = "REDACTED - you do not have read permission on all DAGs in the file"


def _can_read_unparsed_file_import_error(filename: str) -> bool:
    if not conf.getboolean("webserver", "rbac_autoregister_per_folder_roles", fallback=False):
        return False

    from airflow.composer.dag_rbac_per_folder import user_has_dag_file_role

    return user_has_dag_file_role(filename, g.user)


def _get_visible_dag_filelocs(readable_dag_ids: set[str], session: Session) -> set[str]:
    return set(
        session.scalars(select(DagModel.fileloc).distinct().where(DagModel.dag_id.in_(readable_dag_ids)))
    )


def _can_read_import_error(import_error: ParseImportError, visible_dag_filelocs: set[str]) -> bool:
    return import_error.filename in visible_dag_filelocs or _can_read_unparsed_file_import_error(
        import_error.filename
    )


def _redact_import_error_stacktrace_if_needed(
    import_error: ParseImportError,
    readable_dag_ids: set[str],
    session: Session,
) -> None:
    file_dag_ids = set(
        session.scalars(select(DagModel.dag_id).where(DagModel.fileloc == import_error.filename))
    )

    if not file_dag_ids:
        return

    if not file_dag_ids.issubset(readable_dag_ids):
        if object_session(import_error) is not None:
            session.expunge(import_error)
        import_error.stacktrace = REDACTED_IMPORT_ERROR_STACKTRACE


@security.requires_access_view(AccessView.IMPORT_ERRORS)
@provide_session
def get_import_error(*, import_error_id: int, session: Session = NEW_SESSION) -> APIResponse:
    """Get an import error."""
    error = session.get(ParseImportError, import_error_id)
    if error is None:
        raise NotFound(
            "Import error not found",
            detail=f"The ImportError with import_error_id: `{import_error_id}` was not found",
        )
    session.expunge(error)

    can_read_all_dags = get_auth_manager().is_authorized_dag(method="GET")
    if not can_read_all_dags:
        readable_dag_ids = security.get_readable_dags()
        visible_dag_filelocs = _get_visible_dag_filelocs(readable_dag_ids, session)
        if not _can_read_import_error(error, visible_dag_filelocs):
            raise PermissionDenied(detail="You do not have read permission on any of the DAGs in the file")
        _redact_import_error_stacktrace_if_needed(error, readable_dag_ids, session)

    return import_error_schema.dump(error)


@security.requires_access_view(AccessView.IMPORT_ERRORS)
@format_parameters({"limit": check_limit})
@provide_session
def get_import_errors(
    *,
    limit: int,
    offset: int | None = None,
    order_by: str = "import_error_id",
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get all import errors."""
    to_replace = {"import_error_id": "id"}
    allowed_sort_attrs = ["import_error_id", "timestamp", "filename"]
    count_query = select(func.count(ParseImportError.id))
    query = select(ParseImportError)
    query = apply_sorting(query, order_by, to_replace, allowed_sort_attrs)

    can_read_all_dags = get_auth_manager().is_authorized_dag(method="GET")

    if can_read_all_dags:
        total_entries = session.scalars(count_query).one()
        import_errors = session.scalars(query.offset(offset).limit(limit)).all()
    else:
        readable_dag_ids = security.get_readable_dags()
        visible_dag_filelocs = _get_visible_dag_filelocs(readable_dag_ids, session)
        all_import_errors = session.scalars(query).all()
        visible_import_errors = []
        for import_error in all_import_errors:
            if not _can_read_import_error(import_error, visible_dag_filelocs):
                continue
            _redact_import_error_stacktrace_if_needed(import_error, readable_dag_ids, session)
            visible_import_errors.append(import_error)
        total_entries = len(visible_import_errors)
        import_errors = (
            visible_import_errors[offset : offset + limit] if offset else visible_import_errors[:limit]
        )

    return import_error_collection_schema.dump(
        ImportErrorCollection(import_errors=import_errors, total_entries=total_entries)
    )
