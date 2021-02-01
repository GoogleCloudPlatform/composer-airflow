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
import unittest

from tests.test_utils.config import conf_vars


class TestWebserverConfig(unittest.TestCase):
    def test_webserver_config(self):
        with conf_vars(
            {
                ("webserver", "google_oauth2_audience"): "audience",
                ("webserver", "rbac_user_registration_role"): "Viewer",
            }
        ):
            from airflow.composer import webserver_config

        # Check some properties from Composer specific config.
        assert webserver_config.AUTH_ROLE_ADMIN == "Admin"
        assert webserver_config.AUTH_USER_REGISTRATION is False
        assert webserver_config.AUTH_USER_REGISTRATION_ROLE == "Viewer"
        assert webserver_config.CSRF_ENABLED is True
