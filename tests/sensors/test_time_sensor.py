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

from datetime import datetime, time
from unittest.mock import patch

import freezegun
import pendulum
import pytest
from parameterized import parameterized

from airflow.exceptions import TaskDeferred
from airflow.models.dag import DAG
from airflow.sensors.time_sensor import TimeSensor, TimeSensorAsync
from airflow.triggers.temporal import DateTimeTrigger
from airflow.utils import timezone

DEFAULT_TIMEZONE = "Asia/Singapore"  # UTC+08:00
DEFAULT_DATE_WO_TZ = datetime(2015, 1, 1)
DEFAULT_DATE_WITH_TZ = datetime(2015, 1, 1, tzinfo=pendulum.tz.timezone(DEFAULT_TIMEZONE))


@patch(
    "airflow.sensors.time_sensor.timezone.utcnow",
    return_value=timezone.datetime(2020, 1, 1, 23, 0).replace(tzinfo=timezone.utc),
)
class TestTimeSensor:
    @parameterized.expand(
        [
            ("UTC", DEFAULT_DATE_WO_TZ, True),
            ("UTC", DEFAULT_DATE_WITH_TZ, False),
            (DEFAULT_TIMEZONE, DEFAULT_DATE_WO_TZ, False),
        ]
    )
    def test_timezone(self, mock_utcnow, default_timezone, start_date, expected):
        with patch("airflow.settings.TIMEZONE", pendulum.timezone(default_timezone)):
            dag = DAG("test", default_args={"start_date": start_date})
            op = TimeSensor(task_id="test", target_time=time(10, 0), dag=dag)
            assert op.poke(None) == expected


class TestTimeSensorAsync:
    @freezegun.freeze_time("2020-07-07 00:00:00")
    def test_task_is_deferred(self):
        with DAG("test_task_is_deferred", start_date=timezone.datetime(2020, 1, 1, 23, 0)):
            op = TimeSensorAsync(task_id="test", target_time=time(10, 0))
        assert not timezone.is_naive(op.target_datetime)

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute({})

        assert isinstance(exc_info.value.trigger, DateTimeTrigger)
        assert exc_info.value.trigger.moment == timezone.datetime(2020, 7, 7, 10)
        assert exc_info.value.method_name == "execute_complete"
        assert exc_info.value.kwargs is None

    @patch.dict("os.environ", {"COMPOSER_VERSION": "2.15.2"})
    def test_task_fails_to_defer(self):
        from airflow.exceptions import AirflowException

        with DAG("test_task_fails_to_defer", start_date=timezone.datetime(2020, 1, 1, 23, 0)):
            op = TimeSensorAsync(task_id="test", target_time=time(10, 0))

        with pytest.raises(AirflowException) as exc_info:
            op.execute({})

        assert str(exc_info.value) == (
            "This Composer environment does not have Airflow triggerer running. "
            "To use deferrable operators enable the triggerer in the environment. "
            "See https://cloud.google.com/composer/docs/composer-2/use-deferrable-operators "
            "for more details."
        )

    @patch.dict("os.environ", {"COMPOSER_VERSION": "2.15.2"})
    def test_task_is_deferred_when_triggerer_enabled(self):
        with DAG("test_task_is_deferred",
                 start_date=timezone.datetime(2020, 1, 1, 23, 0)):
            op = TimeSensorAsync(task_id="test", target_time=time(10, 0))

        with patch("airflow.configuration.conf.getboolean", autospec=True) \
            as mock:

            def mock_get_boolean(section, key, **kwargs):
                return (section == "composer_internal" and
                        key == "enable_triggerer")

            mock.side_effect = mock_get_boolean
            with pytest.raises(TaskDeferred) as exc_info:
                op.execute({})

            mock.assert_called_once_with(section="composer_internal",
                                         key="enable_triggerer",
                                         fallback=False)

        assert isinstance(exc_info.value.trigger, DateTimeTrigger)
