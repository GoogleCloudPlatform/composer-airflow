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
from unittest import mock

from airflow.providers.google.ads.operators.ads import GoogleAdsToGcsOperator

CLIENT_IDS = ["1111111111", "2222222222"]
BUCKET = "gs://test-google-ads-bucket"
GCS_OBJ_PATH = "folder_name/google-ads-api-results.csv"
QUERY = """
    SELECT
        segments.date,
        customer.id,
    FROM
        ad_group_ad
    WHERE
        segments.date >= '2020-02-01'
        AND segments.date <= '2020-02-29'
    """

FIELDS_TO_EXTRACT = ["segments.date.value", "customer.id.value"]

gcp_conn_id = "gcp_conn_id"
google_ads_conn_id = "google_ads_conn_id"


class TestGoogleAdsToGcsOperator:
    @mock.patch("airflow.providers.google.ads.operators.ads.GoogleAdsHook")
    @mock.patch("airflow.providers.google.ads.operators.ads.GCSHook")
    def test_execute(self, mock_gcs_hook, mock_ads_hook):
        op = GoogleAdsToGcsOperator(
            gcp_conn_id=gcp_conn_id,
            google_ads_conn_id=google_ads_conn_id,
            client_ids=CLIENT_IDS,
            query=QUERY,
            attributes=FIELDS_TO_EXTRACT,
            obj=GCS_OBJ_PATH,
            bucket=BUCKET,
            task_id="run_operator",
        )
        op.execute({})
        mock_ads_hook.assert_called_once_with(
            gcp_conn_id=gcp_conn_id, google_ads_conn_id=google_ads_conn_id
        )
        mock_ads_hook.return_value.search.assert_called_once_with(
            client_ids=CLIENT_IDS, query=QUERY, page_size=10000
        )
        mock_gcs_hook.assert_called_once_with(gcp_conn_id=gcp_conn_id)
        mock_gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET, object_name=GCS_OBJ_PATH, filename=mock.ANY, gzip=False
        )
