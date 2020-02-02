#!/usr/bin/env python
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
"""
Helpers to perform system tests for the Google Campaign Manager.
"""
import os

from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor

BUCKET = os.environ.get("MARKETING_BUCKET", "test-cm-bucket")


class GoogleCampaignManagerTestHelper(LoggingCommandExecutor):
    """
    Helper class to perform system tests for the Google Campaign Manager.
    """

    def create_bucket(self):
        """Create a bucket."""
        self.execute_cmd(["gsutil", "mb", "gs://{}".format(BUCKET)])

    def delete_bucket(self):
        """Delete bucket in Google Cloud Storage service"""
        self.execute_cmd(["gsutil", "rb", "gs://{}".format(BUCKET)])
