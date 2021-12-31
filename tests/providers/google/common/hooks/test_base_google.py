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
#
import json
import os
import re
import unittest
from io import StringIO
from unittest import mock

import google.auth
import pytest
import tenacity
from google.auth.environment_vars import CREDENTIALS
from google.auth.exceptions import GoogleAuthError
from google.cloud.exceptions import Forbidden
from parameterized import parameterized

from airflow import version
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.utils.credentials_provider import _DEFAULT_SCOPES
from airflow.providers.google.common.hooks import base_google as hook
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

default_creds_available = True
default_project = None
try:
    _, default_project = google.auth.default(scopes=_DEFAULT_SCOPES)
except GoogleAuthError:
    default_creds_available = False

MODULE_NAME = "airflow.providers.google.common.hooks.base_google"
PROJECT_ID = "PROJECT_ID"


class NoForbiddenAfterCount:
    """Holds counter state for invoking a method several times in a row."""

    def __init__(self, count, **kwargs):
        self.counter = 0
        self.count = count
        self.kwargs = kwargs

    def __call__(self):
        """
        Raise an Forbidden until after count threshold has been crossed.
        Then return True.
        """
        if self.counter < self.count:
            self.counter += 1
            raise Forbidden(**self.kwargs)
        return True


@hook.GoogleBaseHook.quota_retry(wait=tenacity.wait_none())
def _retryable_test_with_temporary_quota_retry(thing):
    return thing()


class QuotaRetryTestCase(unittest.TestCase):  # ptlint: disable=invalid-name
    def test_do_nothing_on_non_error(self):
        result = _retryable_test_with_temporary_quota_retry(lambda: 42)
        assert result, 42

    def test_retry_on_exception(self):
        message = "POST https://translation.googleapis.com/language/translate/v2: User Rate Limit Exceeded"
        errors = [mock.MagicMock(details=mock.PropertyMock(return_value='userRateLimitExceeded'))]
        custom_fn = NoForbiddenAfterCount(count=5, message=message, errors=errors)
        _retryable_test_with_temporary_quota_retry(custom_fn)
        assert 5 == custom_fn.counter

    def test_raise_exception_on_non_quota_exception(self):
        with pytest.raises(Forbidden, match="Daily Limit Exceeded"):
            message = "POST https://translation.googleapis.com/language/translate/v2: Daily Limit Exceeded"
            errors = [mock.MagicMock(details=mock.PropertyMock(return_value='dailyLimitExceeded'))]

            _retryable_test_with_temporary_quota_retry(
                NoForbiddenAfterCount(5, message=message, errors=errors)
            )


class FallbackToDefaultProjectIdFixtureClass:
    def __init__(self, project_id):
        self.mock = mock.Mock()
        self.fixture_project_id = project_id

    @hook.GoogleBaseHook.fallback_to_default_project_id
    def method(self, project_id=None):
        self.mock(project_id=project_id)

    @property
    def project_id(self):
        return self.fixture_project_id


class TestFallbackToDefaultProjectId(unittest.TestCase):
    def test_no_arguments(self):
        gcp_hook = FallbackToDefaultProjectIdFixtureClass(321)

        gcp_hook.method()

        gcp_hook.mock.assert_called_once_with(project_id=321)

    def test_default_project_id(self):
        gcp_hook = FallbackToDefaultProjectIdFixtureClass(321)

        gcp_hook.method(project_id=None)

        gcp_hook.mock.assert_called_once_with(project_id=321)

    def test_provided_project_id(self):
        gcp_hook = FallbackToDefaultProjectIdFixtureClass(321)

        gcp_hook.method(project_id=123)

        gcp_hook.mock.assert_called_once_with(project_id=123)

    def test_restrict_positional_arguments(self):
        gcp_hook = FallbackToDefaultProjectIdFixtureClass(321)

        with pytest.raises(AirflowException) as ctx:
            gcp_hook.method(123)

        assert str(ctx.value) == "You must use keyword arguments in this methods rather than positional"
        assert gcp_hook.mock.call_count == 0


ENV_VALUE = "/tmp/a"


class TestProvideGcpCredentialFile(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            MODULE_NAME + '.GoogleBaseHook.__init__',
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.instance = hook.GoogleBaseHook(gcp_conn_id="google-cloud-default")

    def test_provide_gcp_credential_file_decorator_key_path_and_keyfile_dict(self):
        key_path = '/test/key-path'
        self.instance.extras = {
            'extra__google_cloud_platform__key_path': key_path,
            'extra__google_cloud_platform__keyfile_dict': '{"foo": "bar"}',
        }

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            assert os.environ[CREDENTIALS] == key_path

        with pytest.raises(
            AirflowException,
            match='The `keyfile_dict` and `key_path` fields are mutually exclusive. '
            'Please provide only one value.',
        ):
            assert_gcp_credential_file_in_env(self.instance)

    def test_provide_gcp_credential_file_decorator_key_path(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            assert os.environ[CREDENTIALS] == key_path

        assert_gcp_credential_file_in_env(self.instance)

    @mock.patch('tempfile.NamedTemporaryFile')
    def test_provide_gcp_credential_file_decorator_key_content(self, mock_file):
        string_file = StringIO()
        file_content = '{"foo": "bar"}'
        file_name = '/test/mock-file'
        self.instance.extras = {'extra__google_cloud_platform__keyfile_dict': file_content}
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            assert os.environ[CREDENTIALS] == file_name
            assert file_content == string_file.getvalue()

        assert_gcp_credential_file_in_env(self.instance)

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            assert os.environ[CREDENTIALS] == key_path

        assert_gcp_credential_file_in_env(self.instance)
        assert os.environ[CREDENTIALS] == ENV_VALUE

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment_when_exception(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            raise Exception()

        with pytest.raises(Exception):
            assert_gcp_credential_file_in_env(self.instance)

        assert os.environ[CREDENTIALS] == ENV_VALUE

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            assert os.environ[CREDENTIALS] == key_path

        assert_gcp_credential_file_in_env(self.instance)
        assert CREDENTIALS not in os.environ

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment_when_exception(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            raise Exception()

        with pytest.raises(Exception):
            assert_gcp_credential_file_in_env(self.instance)

        assert CREDENTIALS not in os.environ


class TestProvideGcpCredentialFileAsContext(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__',
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.instance = hook.GoogleBaseHook(gcp_conn_id="google-cloud-default")

    def test_provide_gcp_credential_file_decorator_key_path(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        with self.instance.provide_gcp_credential_file_as_context():
            assert os.environ[CREDENTIALS] == key_path

    @mock.patch('tempfile.NamedTemporaryFile')
    def test_provide_gcp_credential_file_decorator_key_content(self, mock_file):
        string_file = StringIO()
        file_content = '{"foo": "bar"}'
        file_name = '/test/mock-file'
        self.instance.extras = {'extra__google_cloud_platform__keyfile_dict': file_content}
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        with self.instance.provide_gcp_credential_file_as_context():
            assert os.environ[CREDENTIALS] == file_name
            assert file_content == string_file.getvalue()

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        with self.instance.provide_gcp_credential_file_as_context():
            assert os.environ[CREDENTIALS] == key_path

        assert os.environ[CREDENTIALS] == ENV_VALUE

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment_when_exception(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        with pytest.raises(Exception):
            with self.instance.provide_gcp_credential_file_as_context():
                raise Exception()

        assert os.environ[CREDENTIALS] == ENV_VALUE

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        with self.instance.provide_gcp_credential_file_as_context():
            assert os.environ[CREDENTIALS] == key_path

        assert CREDENTIALS not in os.environ

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment_when_exception(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        with pytest.raises(Exception):
            with self.instance.provide_gcp_credential_file_as_context():
                raise Exception()

        assert CREDENTIALS not in os.environ


class TestGoogleBaseHook(unittest.TestCase):
    def setUp(self):
        self.instance = hook.GoogleBaseHook()

    @mock.patch(MODULE_NAME + '.get_credentials_and_project_id', return_value=("CREDENTIALS", "PROJECT_ID"))
    def test_get_credentials_and_project_id_with_default_auth(self, mock_get_creds_and_proj_id):
        self.instance.extras = {}
        result = self.instance._get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path=None,
            keyfile_dict=None,
            key_secret_name=None,
            scopes=self.instance.scopes,
            delegate_to=None,
            target_principal=None,
            delegates=None,
        )
        assert ('CREDENTIALS', 'PROJECT_ID') == result

    @mock.patch(MODULE_NAME + '.get_credentials_and_project_id')
    def test_get_credentials_and_project_id_with_service_account_file(self, mock_get_creds_and_proj_id):
        mock_credentials = mock.MagicMock()
        mock_get_creds_and_proj_id.return_value = (mock_credentials, "PROJECT_ID")
        self.instance.extras = {'extra__google_cloud_platform__key_path': "KEY_PATH.json"}
        result = self.instance._get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path='KEY_PATH.json',
            keyfile_dict=None,
            key_secret_name=None,
            scopes=self.instance.scopes,
            delegate_to=None,
            target_principal=None,
            delegates=None,
        )
        assert (mock_credentials, 'PROJECT_ID') == result

    def test_get_credentials_and_project_id_with_service_account_file_and_p12_key(self):
        self.instance.extras = {'extra__google_cloud_platform__key_path': "KEY_PATH.p12"}
        with pytest.raises(AirflowException):
            self.instance._get_credentials_and_project_id()

    def test_get_credentials_and_project_id_with_service_account_file_and_unknown_key(self):
        self.instance.extras = {'extra__google_cloud_platform__key_path': "KEY_PATH.unknown"}
        with pytest.raises(AirflowException):
            self.instance._get_credentials_and_project_id()

    @mock.patch(MODULE_NAME + '.get_credentials_and_project_id')
    def test_get_credentials_and_project_id_with_service_account_info(self, mock_get_creds_and_proj_id):
        mock_credentials = mock.MagicMock()
        mock_get_creds_and_proj_id.return_value = (mock_credentials, "PROJECT_ID")
        service_account = {'private_key': "PRIVATE_KEY"}
        self.instance.extras = {'extra__google_cloud_platform__keyfile_dict': json.dumps(service_account)}
        result = self.instance._get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path=None,
            keyfile_dict=service_account,
            key_secret_name=None,
            scopes=self.instance.scopes,
            delegate_to=None,
            target_principal=None,
            delegates=None,
        )
        assert (mock_credentials, 'PROJECT_ID') == result

    @mock.patch(MODULE_NAME + '.get_credentials_and_project_id')
    def test_get_credentials_and_project_id_with_default_auth_and_delegate(self, mock_get_creds_and_proj_id):
        mock_credentials = mock.MagicMock()
        mock_get_creds_and_proj_id.return_value = (mock_credentials, "PROJECT_ID")
        self.instance.extras = {}
        self.instance.delegate_to = "USER"
        result = self.instance._get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path=None,
            keyfile_dict=None,
            key_secret_name=None,
            scopes=self.instance.scopes,
            delegate_to="USER",
            target_principal=None,
            delegates=None,
        )
        assert (mock_credentials, "PROJECT_ID") == result

    @mock.patch('google.auth.default')
    def test_get_credentials_and_project_id_with_default_auth_and_unsupported_delegate(
        self, mock_auth_default
    ):
        self.instance.delegate_to = "TEST_DELEGATE_TO"
        mock_credentials = mock.MagicMock(spec=google.auth.compute_engine.Credentials)
        mock_auth_default.return_value = (mock_credentials, "PROJECT_ID")

        with pytest.raises(
            AirflowException,
            match=re.escape(
                "The `delegate_to` parameter cannot be used here as the current authentication method "
                "does not support account impersonate. Please use service-account for authorization."
            ),
        ):
            self.instance._get_credentials_and_project_id()

    @mock.patch(MODULE_NAME + '.get_credentials_and_project_id', return_value=("CREDENTIALS", "PROJECT_ID"))
    def test_get_credentials_and_project_id_with_default_auth_and_overridden_project_id(
        self, mock_get_creds_and_proj_id
    ):
        self.instance.extras = {'extra__google_cloud_platform__project': "SECOND_PROJECT_ID"}
        result = self.instance._get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path=None,
            keyfile_dict=None,
            key_secret_name=None,
            scopes=self.instance.scopes,
            delegate_to=None,
            target_principal=None,
            delegates=None,
        )
        assert ("CREDENTIALS", 'SECOND_PROJECT_ID') == result

    def test_get_credentials_and_project_id_with_mutually_exclusive_configuration(
        self,
    ):
        self.instance.extras = {
            'extra__google_cloud_platform__project': "PROJECT_ID",
            'extra__google_cloud_platform__key_path': "KEY_PATH",
            'extra__google_cloud_platform__keyfile_dict': '{"KEY": "VALUE"}',
        }
        with pytest.raises(
            AirflowException,
            match=re.escape(
                "The `keyfile_dict`, `key_path`, and `key_secret_name` fields" "are all mutually exclusive. "
            ),
        ):
            self.instance._get_credentials_and_project_id()

    def test_get_credentials_and_project_id_with_invalid_keyfile_dict(
        self,
    ):
        self.instance.extras = {
            'extra__google_cloud_platform__keyfile_dict': 'INVALID_DICT',
        }
        with pytest.raises(AirflowException, match=re.escape('Invalid key JSON.')):
            self.instance._get_credentials_and_project_id()

    @unittest.skipIf(
        not default_creds_available, 'Default Google Cloud credentials not available to run tests'
    )
    def test_default_creds_with_scopes(self):
        self.instance.extras = {
            'extra__google_cloud_platform__project': default_project,
            'extra__google_cloud_platform__scope': (
                ','.join(
                    (
                        'https://www.googleapis.com/auth/bigquery',
                        'https://www.googleapis.com/auth/devstorage.read_only',
                    )
                )
            ),
        }

        credentials = self.instance._get_credentials()

        if not hasattr(credentials, 'scopes') or credentials.scopes is None:
            # Some default credentials don't have any scopes associated with
            # them, and that's okay.
            return

        scopes = credentials.scopes
        assert 'https://www.googleapis.com/auth/bigquery' in scopes
        assert 'https://www.googleapis.com/auth/devstorage.read_only' in scopes

    @unittest.skipIf(
        not default_creds_available, 'Default Google Cloud credentials not available to run tests'
    )
    def test_default_creds_no_scopes(self):
        self.instance.extras = {'extra__google_cloud_platform__project': default_project}

        credentials = self.instance._get_credentials()

        if not hasattr(credentials, 'scopes') or credentials.scopes is None:
            # Some default credentials don't have any scopes associated with
            # them, and that's okay.
            return

        scopes = credentials.scopes
        assert tuple(_DEFAULT_SCOPES) == tuple(scopes)

    def test_provide_gcp_credential_file_decorator_key_path(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(hook_instance):
            assert os.environ[CREDENTIALS] == key_path

        assert_gcp_credential_file_in_env(self.instance)

    @mock.patch('tempfile.NamedTemporaryFile')
    def test_provide_gcp_credential_file_decorator_key_content(self, mock_file):
        string_file = StringIO()
        file_content = '{"foo": "bar"}'
        file_name = '/test/mock-file'
        self.instance.extras = {'extra__google_cloud_platform__keyfile_dict': file_content}
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(hook_instance):
            assert os.environ[CREDENTIALS] == file_name
            assert file_content == string_file.getvalue()

        assert_gcp_credential_file_in_env(self.instance)

    def test_provided_scopes(self):
        self.instance.extras = {
            'extra__google_cloud_platform__project': default_project,
            'extra__google_cloud_platform__scope': (
                ','.join(
                    (
                        'https://www.googleapis.com/auth/bigquery',
                        'https://www.googleapis.com/auth/devstorage.read_only',
                    )
                )
            ),
        }

        assert self.instance.scopes == [
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/devstorage.read_only',
        ]

    def test_default_scopes(self):
        self.instance.extras = {'extra__google_cloud_platform__project': default_project}

        assert self.instance.scopes == ('https://www.googleapis.com/auth/cloud-platform',)

    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection")
    def test_num_retries_is_not_none_by_default(self, get_con_mock):
        """
        Verify that if 'num_retries' in extras is not set, the default value
        should not be None
        """
        get_con_mock.return_value.extra_dejson = {"extra__google_cloud_platform__num_retries": None}
        assert self.instance.num_retries == 5

    @mock.patch("airflow.providers.google.common.hooks.base_google.build_http")
    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials")
    def test_authorize_assert_user_agent_is_sent(self, mock_get_credentials, mock_http):
        """
        Verify that if 'num_retires' in extras is not set, the default value
        should not be None
        """
        request = mock_http.return_value.request
        response = mock.MagicMock(status_code=200)
        content = "CONTENT"
        mock_http.return_value.request.return_value = response, content

        new_response, new_content = self.instance._authorize().request("/test-action")

        request.assert_called_once_with(
            '/test-action',
            body=None,
            connection_type=None,
            headers={'user-agent': 'airflow/' + version.version},
            method='GET',
            redirections=5,
        )
        assert response == new_response
        assert content == new_content

    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials")
    def test_authorize_assert_http_308_is_excluded(self, mock_get_credentials):
        """
        Verify that 308 status code is excluded from httplib2's redirect codes
        """
        http_authorized = self.instance._authorize().http
        assert 308 not in http_authorized.redirect_codes

    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials")
    def test_authorize_assert_http_timeout_is_present(self, mock_get_credentials):
        """
        Verify that http client has a timeout set
        """
        http_authorized = self.instance._authorize().http
        assert http_authorized.timeout is not None

    @parameterized.expand(
        [
            ('string', "ACCOUNT_1", "ACCOUNT_1", None),
            ('single_element_list', ["ACCOUNT_1"], "ACCOUNT_1", []),
            (
                'multiple_elements_list',
                ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"],
                "ACCOUNT_3",
                ["ACCOUNT_1", "ACCOUNT_2"],
            ),
        ]
    )
    @mock.patch(MODULE_NAME + '.get_credentials_and_project_id')
    def test_get_credentials_and_project_id_with_impersonation_chain(
        self,
        _,
        impersonation_chain,
        target_principal,
        delegates,
        mock_get_creds_and_proj_id,
    ):
        mock_credentials = mock.MagicMock()
        mock_get_creds_and_proj_id.return_value = (mock_credentials, PROJECT_ID)
        self.instance.impersonation_chain = impersonation_chain
        result = self.instance._get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path=None,
            keyfile_dict=None,
            key_secret_name=None,
            scopes=self.instance.scopes,
            delegate_to=None,
            target_principal=target_principal,
            delegates=delegates,
        )
        assert (mock_credentials, PROJECT_ID) == result


class TestProvideAuthorizedGcloud(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            MODULE_NAME + '.GoogleBaseHook.__init__',
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.instance = hook.GoogleBaseHook(gcp_conn_id="google-cloud-default")

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=mock.PropertyMock,
        return_value="PROJECT_ID",
    )
    @mock.patch(MODULE_NAME + '.check_output')
    def test_provide_authorized_gcloud_key_path_and_keyfile_dict(self, mock_check_output, mock_default):
        key_path = '/test/key-path'
        self.instance.extras = {
            'extra__google_cloud_platform__key_path': key_path,
            'extra__google_cloud_platform__keyfile_dict': '{"foo": "bar"}',
        }

        with pytest.raises(
            AirflowException,
            match='The `keyfile_dict` and `key_path` fields are mutually exclusive. '
            'Please provide only one value.',
        ):
            with self.instance.provide_authorized_gcloud():
                assert os.environ[CREDENTIALS] == key_path

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=mock.PropertyMock,
        return_value="PROJECT_ID",
    )
    @mock.patch(MODULE_NAME + '.check_output')
    def test_provide_authorized_gcloud_key_path(self, mock_check_output, mock_project_id):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        with self.instance.provide_authorized_gcloud():
            assert os.environ[CREDENTIALS] == key_path

        mock_check_output.has_calls(
            mock.call(['gcloud', 'config', 'set', 'core/project', 'PROJECT_ID']),
            mock.call(['gcloud', 'auth', 'activate-service-account', '--key-file=/test/key-path']),
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=mock.PropertyMock,
        return_value="PROJECT_ID",
    )
    @mock.patch(MODULE_NAME + '.check_output')
    @mock.patch('tempfile.NamedTemporaryFile')
    def test_provide_authorized_gcloud_keyfile_dict(self, mock_file, mock_check_output, mock_project_id):
        string_file = StringIO()
        file_content = '{"foo": "bar"}'
        file_name = '/test/mock-file'
        self.instance.extras = {'extra__google_cloud_platform__keyfile_dict': file_content}
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        with self.instance.provide_authorized_gcloud():
            assert os.environ[CREDENTIALS] == file_name

        mock_check_output.has_calls(
            [
                mock.call(['gcloud', 'config', 'set', 'core/project', 'PROJECT_ID']),
                mock.call(['gcloud', 'auth', 'activate-service-account', '--key-file=/test/mock-file']),
            ]
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=mock.PropertyMock,
        return_value="PROJECT_ID",
    )
    @mock.patch(MODULE_NAME + '._cloud_sdk')
    @mock.patch(MODULE_NAME + '.check_output')
    @mock.patch('tempfile.NamedTemporaryFile')
    def test_provide_authorized_gcloud_via_gcloud_application_default(
        self, mock_file, mock_check_output, mock_cloud_sdk, mock_project_id
    ):
        # This file always exists.
        mock_cloud_sdk.get_application_default_credentials_path.return_value = __file__

        file_content = json.dumps(
            {
                "client_id": "CLIENT_ID",
                "client_secret": "CLIENT_SECRET",
                "refresh_token": "REFRESH_TOKEN",
                "type": "authorized_user",
            }
        )
        with mock.patch(MODULE_NAME + '.open', mock.mock_open(read_data=file_content)):
            with self.instance.provide_authorized_gcloud():
                # Do nothing
                pass

        mock_check_output.assert_has_calls(
            [
                mock.call(['gcloud', 'config', 'set', 'auth/client_id', 'CLIENT_ID']),
                mock.call(['gcloud', 'config', 'set', 'auth/client_secret', 'CLIENT_SECRET']),
                mock.call(['gcloud', 'auth', 'activate-refresh-token', 'CLIENT_ID', 'REFRESH_TOKEN']),
                mock.call(['gcloud', 'config', 'set', 'core/project', 'PROJECT_ID']),
            ],
            any_order=False,
        )


class TestNumRetry(unittest.TestCase):
    def test_should_return_int_when_set_int_via_connection(self):
        instance = hook.GoogleBaseHook(gcp_conn_id="google_cloud_default")
        instance.extras = {
            'extra__google_cloud_platform__num_retries': 10,
        }

        assert isinstance(instance.num_retries, int)
        assert 10 == instance.num_retries

    @mock.patch.dict(
        'os.environ',
        AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=(
            'google-cloud-platform://?extra__google_cloud_platform__num_retries=5'
        ),
    )
    def test_should_return_int_when_set_via_env_var(self):
        instance = hook.GoogleBaseHook(gcp_conn_id="google_cloud_default")
        assert isinstance(instance.num_retries, int)

    @mock.patch.dict(
        'os.environ',
        AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=(
            'google-cloud-platform://?extra__google_cloud_platform__num_retries=cat'
        ),
    )
    def test_should_raise_when_invalid_value_via_env_var(self):
        instance = hook.GoogleBaseHook(gcp_conn_id="google_cloud_default")
        with pytest.raises(AirflowException, match=re.escape("The num_retries field should be a integer.")):
            assert isinstance(instance.num_retries, int)

    @mock.patch.dict(
        'os.environ',
        AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=(
            'google-cloud-platform://?extra__google_cloud_platform__num_retries='
        ),
    )
    def test_should_fallback_when_empty_string_in_env_var(self):
        instance = hook.GoogleBaseHook(gcp_conn_id="google_cloud_default")
        assert isinstance(instance.num_retries, int)
        assert 5 == instance.num_retries
