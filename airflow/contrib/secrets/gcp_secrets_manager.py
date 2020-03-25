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

"""
Objects relating to sourcing connections from GCP Secrets Manager
"""
from typing import Optional

from cached_property import cached_property
from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.client_info import ClientInfo
from google.cloud.secretmanager_v1 import SecretManagerServiceClient

from airflow import version

from airflow.contrib.utils.gcp_credentials_provider import (
    _get_scopes, get_credentials_and_project_id,
)
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


class CloudSecretsManagerBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connection object from GCP Secrets Manager

    Configurable via ``airflow.cfg`` as follows:

    .. code-block:: ini

        [secrets]
        backend = airflow.contrib.secrets.gcp_secrets_manager.CloudSecretsManagerBackend
        backend_kwargs = {"connections_prefix": "airflow/connections"}

    For example, if secret id is ``airflow/connections/smtp_default``, this would be accessible
    if you provide ``{"connections_prefix": "airflow/connections"}`` and request conn_id ``smtp_default``.

    :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
    :type connections_prefix: str
    :param gcp_key_path: Path to GCP Credential JSON file;
        use default credentials in the current environment if not provided.
    :type gcp_key_path: str
    :param gcp_scopes: Comma-separated string containing GCP scopes
    :type gcp_scopes: str
    """
    def __init__(
        self,
        connections_prefix="airflow/connections",  # type: str
        gcp_key_path=None,  # type: Optional[str]
        gcp_scopes=None,  # type: Optional[str]
        **kwargs
    ):
        self.connections_prefix = connections_prefix.rstrip("/")
        self.gcp_key_path = gcp_key_path
        self.gcp_scopes = gcp_scopes
        self.credentials = None
        self.project_id = None
        super(CloudSecretsManagerBackend, self).__init__(**kwargs)

    @cached_property
    def client(self):
        # type: (...) -> SecretManagerServiceClient
        """
        Create an authenticated KMS client
        """
        scopes = _get_scopes(self.gcp_scopes)
        self.credentials, self.project_id = get_credentials_and_project_id(
            key_path=self.gcp_key_path,
            scopes=scopes
        )
        _client = SecretManagerServiceClient(
            credentials=self.credentials,
            client_info=ClientInfo(client_library_version='airflow_v' + version.version)
        )
        return _client

    def get_conn_uri(self, conn_id):
        """
        Get secret value from Secrets Manager.

        :param conn_id: connection id
        :type conn_id: str
        """
        secret_id = self.build_path(connections_prefix=self.connections_prefix, conn_id=conn_id)
        # always return the latest version of the secret
        secret_version = "latest"
        name = self.client.secret_version_path(self.project_id, secret_id, secret_version)
        try:
            response = self.client.access_secret_version(name)
            value = response.payload.data.decode('UTF-8')
            return value
        except NotFound:
            self.log.error(
                "GCP API Call Error (NotFound): Secret ID %s not found.", secret_id
            )
            return None
