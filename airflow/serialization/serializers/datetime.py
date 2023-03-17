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
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.utils.module_loading import qualname
from airflow.utils.timezone import convert_to_utc, is_naive

if TYPE_CHECKING:
    import datetime

    from airflow.serialization.serde import U

__version__ = 1

serializers = ["datetime.date", "datetime.datetime", "datetime.timedelta", "pendulum.datetime.DateTime"]
deserializers = serializers

TIMESTAMP = "timestamp"
TIMEZONE = "tz"


def serialize(o: object) -> tuple[U, str, int, bool]:
    from datetime import date, datetime, timedelta

    if isinstance(o, datetime):
        qn = qualname(o)
        if is_naive(o):
            o = convert_to_utc(o)

        tz = o.tzname()

        return {TIMESTAMP: o.timestamp(), TIMEZONE: tz}, qn, __version__, True

    if isinstance(o, date):
        return o.isoformat(), qualname(o), __version__, True

    if isinstance(o, timedelta):
        return o.total_seconds(), qualname(o), __version__, True

    return "", "", 0, False


def deserialize(classname: str, version: int, data: dict | str) -> datetime.date | datetime.timedelta:
    import datetime

    from pendulum import DateTime
    from pendulum.tz import timezone

    if classname == qualname(datetime.datetime) and isinstance(data, dict):
        return datetime.datetime.fromtimestamp(float(data[TIMESTAMP]), tz=timezone(data[TIMEZONE]))

    if classname == qualname(DateTime) and isinstance(data, dict):
        return DateTime.fromtimestamp(float(data[TIMESTAMP]), tz=timezone(data[TIMEZONE]))

    if classname == qualname(datetime.timedelta) and isinstance(data, (str, float)):
        return datetime.timedelta(seconds=float(data))

    if classname == qualname(datetime.date) and isinstance(data, str):
        return datetime.date.fromisoformat(data)

    raise TypeError(f"unknown date/time format {classname}")
