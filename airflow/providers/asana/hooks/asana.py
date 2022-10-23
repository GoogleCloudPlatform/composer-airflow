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
"""Connect to Asana."""
from __future__ import annotations

from functools import wraps
from typing import Any

from asana import Client  # type: ignore[attr-defined]
from asana.error import NotFoundError  # type: ignore[attr-defined]

from airflow.compat.functools import cached_property
from airflow.hooks.base import BaseHook


def _ensure_prefixes(conn_type):
    """
    Remove when provider min airflow version >= 2.5.0 since this is handled by
    provider manager from that version.
    """

    def dec(func):
        @wraps(func)
        def inner():
            field_behaviors = func()
            conn_attrs = {"host", "schema", "login", "password", "port", "extra"}

            def _ensure_prefix(field):
                if field not in conn_attrs and not field.startswith("extra__"):
                    return f"extra__{conn_type}__{field}"
                else:
                    return field

            if "placeholders" in field_behaviors:
                placeholders = field_behaviors["placeholders"]
                field_behaviors["placeholders"] = {_ensure_prefix(k): v for k, v in placeholders.items()}
            return field_behaviors

        return inner

    return dec


class AsanaHook(BaseHook):
    """Wrapper around Asana Python client library."""

    conn_name_attr = "asana_conn_id"
    default_conn_name = "asana_default"
    conn_type = "asana"
    hook_name = "Asana"

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.connection = self.get_connection(conn_id)
        extras = self.connection.extra_dejson
        self.workspace = self._get_field(extras, "workspace") or None
        self.project = self._get_field(extras, "project") or None

    def _get_field(self, extras: dict, field_name: str):
        """Get field from extra, first checking short name, then for backcompat we check for prefixed name."""
        backcompat_prefix = "extra__asana__"
        if field_name.startswith("extra__"):
            raise ValueError(
                f"Got prefixed name {field_name}; please remove the '{backcompat_prefix}' prefix "
                "when using this method."
            )
        if field_name in extras:
            return extras[field_name] or None
        prefixed_name = f"{backcompat_prefix}{field_name}"
        return extras.get(prefixed_name) or None

    def get_conn(self) -> Client:
        return self.client

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "workspace": StringField(lazy_gettext("Workspace"), widget=BS3TextFieldWidget()),
            "project": StringField(lazy_gettext("Project"), widget=BS3TextFieldWidget()),
        }

    @staticmethod
    @_ensure_prefixes(conn_type="asana")
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["port", "host", "login", "schema"],
            "relabeling": {},
            "placeholders": {
                "password": "Asana personal access token",
                "workspace": "Asana workspace gid",
                "project": "Asana project gid",
            },
        }

    @cached_property
    def client(self) -> Client:
        """Instantiates python-asana Client"""
        if not self.connection.password:
            raise ValueError(
                "Asana connection password must contain a personal access token: "
                "https://developers.asana.com/docs/personal-access-token"
            )

        return Client.access_token(self.connection.password)

    def create_task(self, task_name: str, params: dict | None) -> dict:
        """
        Creates an Asana task.

        :param task_name: Name of the new task
        :param params: Other task attributes, such as due_on, parent, and notes. For a complete list
            of possible parameters, see https://developers.asana.com/docs/create-a-task
        :return: A dict of attributes of the created task, including its gid
        """
        merged_params = self._merge_create_task_parameters(task_name, params)
        self._validate_create_task_parameters(merged_params)
        response = self.client.tasks.create(params=merged_params)
        return response

    def _merge_create_task_parameters(self, task_name: str, task_params: dict | None) -> dict:
        """
        Merge create_task parameters with default params from the connection.

        :param task_name: Name of the task
        :param task_params: Other task parameters which should override defaults from the connection
        :return: A dict of merged parameters to use in the new task
        """
        merged_params: dict[str, Any] = {"name": task_name}
        if self.project:
            merged_params["projects"] = [self.project]
        # Only use default workspace if user did not provide a project id
        elif self.workspace and not (task_params and ("projects" in task_params)):
            merged_params["workspace"] = self.workspace
        if task_params:
            merged_params.update(task_params)
        return merged_params

    @staticmethod
    def _validate_create_task_parameters(params: dict) -> None:
        """
        Check that user provided minimal parameters for task creation.

        :param params: A dict of attributes the task to be created should have
        :return: None; raises ValueError if `params` doesn't contain required parameters
        """
        required_parameters = {"workspace", "projects", "parent"}
        if required_parameters.isdisjoint(params):
            raise ValueError(
                f"You must specify at least one of {required_parameters} in the create_task parameters"
            )

    def delete_task(self, task_id: str) -> dict:
        """
        Deletes an Asana task.

        :param task_id: Asana GID of the task to delete
        :return: A dict containing the response from Asana
        """
        try:
            response = self.client.tasks.delete_task(task_id)
            return response
        except NotFoundError:
            self.log.info("Asana task %s not found for deletion.", task_id)
            return {}

    def find_task(self, params: dict | None) -> list:
        """
        Retrieves a list of Asana tasks that match search parameters.

        :param params: Attributes that matching tasks should have. For a list of possible parameters,
            see https://developers.asana.com/docs/get-multiple-tasks
        :return: A list of dicts containing attributes of matching Asana tasks
        """
        merged_params = self._merge_find_task_parameters(params)
        self._validate_find_task_parameters(merged_params)
        response = self.client.tasks.find_all(params=merged_params)
        return list(response)

    def _merge_find_task_parameters(self, search_parameters: dict | None) -> dict:
        """
        Merge find_task parameters with default params from the connection.

        :param search_parameters: Attributes that tasks matching the search should have; these override
            defaults from the connection
        :return: A dict of merged parameters to use in the search
        """
        merged_params = {}
        if self.project:
            merged_params["project"] = self.project
        # Only use default workspace if user did not provide a project id
        elif self.workspace and not (search_parameters and ("project" in search_parameters)):
            merged_params["workspace"] = self.workspace
        if search_parameters:
            merged_params.update(search_parameters)
        return merged_params

    @staticmethod
    def _validate_find_task_parameters(params: dict) -> None:
        """
        Check that the user provided minimal search parameters.

        :param params: Dict of parameters to be used in the search
        :return: None; raises ValueError if search parameters do not contain minimum required attributes
        """
        one_of_list = {"project", "section", "tag", "user_task_list"}
        both_of_list = {"assignee", "workspace"}
        contains_both = both_of_list.issubset(params)
        contains_one = not one_of_list.isdisjoint(params)
        if not (contains_both or contains_one):
            raise ValueError(
                f"You must specify at least one of {one_of_list} "
                f"or both of {both_of_list} in the find_task parameters."
            )

    def update_task(self, task_id: str, params: dict) -> dict:
        """
        Updates an existing Asana task.

        :param task_id: Asana GID of task to update
        :param params: New values of the task's attributes. For a list of possible parameters, see
            https://developers.asana.com/docs/update-a-task
        :return: A dict containing the updated task's attributes
        """
        response = self.client.tasks.update(task_id, params)
        return response

    def create_project(self, params: dict) -> dict:
        """
        Creates a new project.

        :param params: Attributes that the new project should have. See
            https://developers.asana.com/docs/create-a-project#create-a-project-parameters
            for a list of possible parameters.
        :return: A dict containing the new project's attributes, including its GID.
        """
        merged_params = self._merge_project_parameters(params)
        self._validate_create_project_parameters(merged_params)
        response = self.client.projects.create(merged_params)
        return response

    @staticmethod
    def _validate_create_project_parameters(params: dict) -> None:
        """
        Check that user provided the minimum required parameters for project creation

        :param params: Attributes that the new project should have
        :return: None; raises a ValueError if `params` does not contain the minimum required attributes.
        """
        required_parameters = {"workspace", "team"}
        if required_parameters.isdisjoint(params):
            raise ValueError(
                f"You must specify at least one of {required_parameters} in the create_project params"
            )

    def _merge_project_parameters(self, params: dict) -> dict:
        """
        Merge parameters passed into a project method with default params from the connection.

        :param params: Parameters passed into one of the project methods, which should override
            defaults from the connection
        :return: A dict of merged parameters
        """
        merged_params = {} if self.workspace is None else {"workspace": self.workspace}
        merged_params.update(params)
        return merged_params

    def find_project(self, params: dict) -> list:
        """
        Retrieves a list of Asana projects that match search parameters.

        :param params: Attributes which matching projects should have. See
            https://developers.asana.com/docs/get-multiple-projects
            for a list of possible parameters.
        :return: A list of dicts containing attributes of matching Asana projects
        """
        merged_params = self._merge_project_parameters(params)
        response = self.client.projects.find_all(merged_params)
        return list(response)

    def update_project(self, project_id: str, params: dict) -> dict:
        """
        Updates an existing project.

        :param project_id: Asana GID of the project to update
        :param params: New attributes that the project should have. See
            https://developers.asana.com/docs/update-a-project#update-a-project-parameters
            for a list of possible parameters
        :return: A dict containing the updated project's attributes
        """
        response = self.client.projects.update(project_id, params)
        return response

    def delete_project(self, project_id: str) -> dict:
        """
        Deletes a project.

        :param project_id: Asana GID of the project to delete
        :return: A dict containing the response from Asana
        """
        try:
            response = self.client.projects.delete(project_id)
            return response
        except NotFoundError:
            self.log.info("Asana project %s not found for deletion.", project_id)
            return {}
