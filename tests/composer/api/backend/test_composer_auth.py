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
import os
import random
import shutil
import unittest
from unittest import mock

from airflow.configuration import WEBSERVER_CONFIG
from airflow.www import app
from tests.test_utils.config import conf_vars


class TestComposerAuth(unittest.TestCase):
    # TODO: setup of application for tests that involves Composer security
    # manager is quite similar (see test_security_manager.py), so we should
    # refactor this to extract it into pytest.fixture for better usage and
    # avoid of code duplication.
    CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))
    WEBSERVER_CONFIG_BACKUP = WEBSERVER_CONFIG + ".backup"
    COMPOSER_WEBSERVER_CONFIG = os.path.join(
        CURRENT_DIRECTORY, "../../../../airflow/composer/webserver_config.py"
    )

    @classmethod
    def setUpClass(cls):
        # Override webserver config with Composer specific.
        shutil.copy(WEBSERVER_CONFIG, cls.WEBSERVER_CONFIG_BACKUP)
        shutil.copy(cls.COMPOSER_WEBSERVER_CONFIG, WEBSERVER_CONFIG)
        with conf_vars(
            {
                ("webserver", "google_oauth2_audience"): "audience",
                ("webserver", "rbac_user_registration_role"): "Viewer",
                ("api", "auth_backend"): "airflow.composer.api.backend.composer_auth",
                ("api", "composer_auth_user_registration_role"): "User",
            }
        ):
            cls.app = app.create_app(testing=True)

        cls.test_client = cls.app.test_client()

    @classmethod
    def tearDownClass(cls):
        # Return back original webserver config.
        shutil.copy(cls.WEBSERVER_CONFIG_BACKUP, WEBSERVER_CONFIG)

    @mock.patch("airflow.composer.security_manager._decode_iap_jwt", autospec=True)
    def test_authentication_success(self, _decode_iap_jwt_mock):
        def _decode_iap_jwt_mock_side_effect(iap_jwt):
            assert iap_jwt == "jwt-test"
            username = f"test-composer-auth-{random.random()}"
            return username, f"{username}@test.com"

        _decode_iap_jwt_mock.side_effect = _decode_iap_jwt_mock_side_effect

        # In both requests user has to be self-registered with "User" role.
        import_errors_response = self.test_client.get(
            "/api/v1/importErrors", headers={"X-Goog-IAP-JWT-Assertion": "jwt-test"}
        )
        pools_response = self.test_client.get(
            "/api/v1/pools", headers={"X-Goog-IAP-JWT-Assertion": "jwt-test"}
        )

        # "User" role have access to importErrors endpoint.
        assert import_errors_response.status_code == 200
        assert "import_errors" in import_errors_response.json
        assert "total_entries" in import_errors_response.json
        # "User" role doesn't have access to pools endpoint.
        assert pools_response.status_code == 403

    def test_authentication_failure(self):
        response = self.test_client.get(
            "/api/v1/pools", headers={"X-Goog-IAP-JWT-Assertion": "invalid-jwt-token"}
        )

        assert response.status_code == 401
        assert response.headers["WWW-Authenticate"] == "Composer"
