#
# Copyright 2025 Google LLC
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
from __future__ import annotations

import warnings


def filter_warnings():
    """Filter warnings that are not relevant for customer/Composer, i.e. do not require any action items."""
    # This warning is produced by flask rate limiter. It warns that we must choose a storage backend to store
    # rate limiting information instead of the default in-memory one. This is not an issue for Composer as
    # the Airflow Webserver is behind Google network, which already protects against such attacks.
    warnings.filterwarnings(
        "ignore",
        r".*Using the in-memory storage for tracking rate limits as no storage was explicitly specified.*",
    )
