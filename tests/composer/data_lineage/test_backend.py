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
import unittest
from unittest import mock

from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.cloud.datacatalog.lineage_v1 import CreateLineageEventsBundleRequest, LineageEventsBundle, Process

from airflow.composer.data_lineage.backend import ComposerDataLineageBackend


class TestBackend(unittest.TestCase):
    @mock.patch("airflow.composer.data_lineage.backend.SyncLineageClient", autospec=True)
    @mock.patch("airflow.composer.data_lineage.backend.ComposerDataLineageAdapter", autospec=True)
    @mock.patch("airflow.composer.data_lineage.backend.uuid", autospec=True)
    @mock.patch("airflow.composer.data_lineage.backend.LOCATION_PATH", "TEST-LOCATION")
    def test_send_lineage(self, mock_uuid, mock_composer_data_lineage_adapter, mock_sync_lineage_client):
        mock_lineage_events_bundle = LineageEventsBundle(process=Process())

        def _mock_get_lineage_events_bundle_on_task_completed(task_instance, inlets, outlets):
            self.assertEqual(task_instance, mock_ti)
            self.assertEqual(inlets, mock_inlets)
            self.assertEqual(outlets, mock_outlets)
            return mock_lineage_events_bundle

        mock_uuid.uuid4().hex = "test-uuid"
        mock_composer_data_lineage_adapter().get_lineage_events_bundle_on_task_completed.side_effect = (
            _mock_get_lineage_events_bundle_on_task_completed
        )
        mock_ti = mock.Mock()
        mock_inlets = mock.Mock()
        mock_outlets = mock.Mock()

        _backend = ComposerDataLineageBackend()
        _backend.send_lineage(
            operator=mock.Mock(),
            inlets=mock_inlets,
            outlets=mock_outlets,
            context={"ti": mock_ti},
        )

        mock_sync_lineage_client().create_events_bundle.assert_called_once_with(
            request=CreateLineageEventsBundleRequest(
                parent="TEST-LOCATION",
                lineage_events_bundle=mock_lineage_events_bundle,
                request_id="test-uuid",
            ),
            retry=mock.ANY,
        )
        self.assertEqual(
            mock_sync_lineage_client().create_events_bundle.call_args_list[0][1]["retry"]._deadline,
            5,
        )

    @mock.patch("airflow.composer.data_lineage.backend.SyncLineageClient", autospec=True)
    @mock.patch("airflow.composer.data_lineage.backend.ComposerDataLineageAdapter", autospec=True)
    @mock.patch("airflow.composer.data_lineage.backend.CreateLineageEventsBundleRequest", autospec=True)
    def test_send_lineage_exception(
        self,
        mock_create_lineage_events_bundle_request,
        mock_composer_data_lineage_adapter,
        mock_sync_lineage_client,
    ):
        mock_sync_lineage_client().create_events_bundle.side_effect = GoogleAPICallError("Error")
        _backend = ComposerDataLineageBackend()

        # Check that send_lineage doesn't raise exception in case of API call error.
        _backend.send_lineage(
            operator=mock.Mock(),
            context={"ti": mock.Mock()},
        )

    @mock.patch("airflow.composer.data_lineage.backend.SyncLineageClient", autospec=True)
    @mock.patch("airflow.composer.data_lineage.backend.ComposerDataLineageAdapter", autospec=True)
    @mock.patch("airflow.composer.data_lineage.backend.CreateLineageEventsBundleRequest", autospec=True)
    def test_send_lineage_exception_retry_deadline(
        self,
        mock_create_lineage_events_bundle_request,
        mock_composer_data_lineage_adapter,
        mock_sync_lineage_client,
    ):
        mock_sync_lineage_client().create_events_bundle.side_effect = RetryError("Error", "cause")
        _backend = ComposerDataLineageBackend()

        # Check that send_lineage doesn't raise exception in case of reaching retry deadline.
        _backend.send_lineage(
            operator=mock.Mock(),
            context={"ti": mock.Mock()},
        )
