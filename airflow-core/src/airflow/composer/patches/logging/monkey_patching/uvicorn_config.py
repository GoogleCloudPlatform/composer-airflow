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

from uvicorn.config import LOGGING_CONFIG


def patch():
    # Set stdout as a stream for default uvicorn handler.
    # By default, uvicorn default handler uses stderr as a stream which means that those logs will be
    # classified as ERROR logs in Cloud Logging, and that is not desired in Composer. Here, we make logs
    # emitted by default handler to go to stdout and severity in Cloud Logging will be determined based on the
    # prefix (e.g. "INFO: ...") of the message.
    LOGGING_CONFIG["handlers"]["default"]["stream"] = "ext://sys.stdout"
