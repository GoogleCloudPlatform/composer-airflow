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
from typing import Any, Callable, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class HttpSensor(BaseSensorOperator):
    """
    Executes a HTTP GET statement and returns False on failure caused by
    404 Not Found or `response_check` returning False.

    HTTP Error codes other than 404 (like 403) or Connection Refused Error
    would fail the sensor itself directly (no more poking).

    The response check can access the template context to the operator:

        def response_check(response, task_instance):
            # The task_instance is injected, so you can pull data form xcom
            # Other context variables such as dag, ds, execution_date are also available.
            xcom_data = task_instance.xcom_pull(task_ids='pushing_task')
            # In practice you would do something more sensible with this data..
            print(xcom_data)
            return True

        HttpSensor(task_id='my_http_sensor', ..., response_check=response_check)

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:HttpSensor`

    :param http_conn_id: The connection to run the sensor against
    :type http_conn_id: str
    :param method: The HTTP request method to use
    :type method: str
    :param endpoint: The relative part of the full url
    :type endpoint: str
    :param request_params: The parameters to be added to the GET url
    :type request_params: a dictionary of string key/value pairs
    :param headers: The HTTP headers to be added to the GET request
    :type headers: a dictionary of string key/value pairs
    :param response_check: A check against the 'requests' response object.
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        It should return True for 'pass' and False otherwise.
    :type response_check: A lambda or defined function.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    """

    template_fields = ('endpoint', 'request_params', 'headers')

    @apply_defaults
    def __init__(
        self,
        *,
        endpoint: str,
        http_conn_id: str = 'http_default',
        method: str = 'GET',
        request_params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        response_check: Optional[Callable[..., bool]] = None,
        extra_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check

        self.hook = HttpHook(method=method, http_conn_id=http_conn_id)

    def poke(self, context: Dict[Any, Any]) -> bool:
        from airflow.utils.operator_helpers import make_kwargs_callable

        self.log.info('Poking: %s', self.endpoint)
        try:
            response = self.hook.run(
                self.endpoint,
                data=self.request_params,
                headers=self.headers,
                extra_options=self.extra_options,
            )
            if self.response_check:
                kwargs_callable = make_kwargs_callable(self.response_check)
                return kwargs_callable(response, **context)

        except AirflowException as exc:
            if str(exc).startswith("404"):
                return False

            raise exc

        return True
