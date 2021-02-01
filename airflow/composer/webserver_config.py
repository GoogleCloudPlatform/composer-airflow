# -*- coding: utf-8 -*-
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
"""Airflow web server config file."""

from flask_appbuilder.const import AUTH_REMOTE_USER

from airflow import configuration as conf
from airflow.composer.security_manager import ComposerAirflowSecurityManager


# The authentication type.
AUTH_TYPE = AUTH_REMOTE_USER

# Set up full admin role name.
AUTH_ROLE_ADMIN = "Admin"

# Composer doesn't use Flask-AppBuilder's self registration. Users are
# automatically registered on their first login, which is safe because access to
# the whole Airflow UI is protected by Cloud IAP.
AUTH_USER_REGISTRATION = False

# The default user self registration role.
AUTH_USER_REGISTRATION_ROLE = conf.get("webserver",
                                       "rbac_user_registration_role")

# The SQLAlchemy connection string (copied from default_webserver_config.py).
SQLALCHEMY_DATABASE_URI = conf.get("core", "SQL_ALCHEMY_CONN")

# Flask-WTF flag for CSRF (copied from default_webserver_config.py).
CSRF_ENABLED = True

SECURITY_MANAGER_CLASS = ComposerAirflowSecurityManager
