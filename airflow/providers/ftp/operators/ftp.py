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
"""This module contains FTP operator."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Sequence

from airflow.compat.functools import cached_property
from airflow.models import BaseOperator
from airflow.providers.ftp.hooks.ftp import FTPHook, FTPSHook


class FTPOperation:
    """Operation that can be used with FTP"""

    PUT = "put"
    GET = "get"


class FTPFileTransmitOperator(BaseOperator):
    """
    FTPFileTransmitOperator for transferring files from remote host to local or vice a versa.
    This operator uses an FTPHook to open ftp transport channel that serve as basis
    for file transfer.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:FTPFileTransmitOperator`

    :param ftp_conn_id: :ref:`ftp connection id<howto/connection:ftp>`
        from airflow Connections.
    :param local_filepath: local file path to get or put. (templated)
    :param remote_filepath: remote file path to get or put. (templated)
    :param operation: specify operation 'get' or 'put', defaults to put
    :param create_intermediate_dirs: create missing intermediate directories when
        copying from remote to local and vice-versa. Default is False.

        Example: The following task would copy ``file.txt`` to the remote host
        at ``/tmp/tmp1/tmp2/`` while creating ``tmp``,``tmp1`` and ``tmp2`` if they
        don't exist. If the ``create_intermediate_dirs`` parameter is not passed it would error
        as the directory does not exist. ::

            put_file = FTPFileTransmitOperator(
                task_id="test_ftp",
                ftp_conn_id="ftp_default",
                local_filepath="/tmp/file.txt",
                remote_filepath="/tmp/tmp1/tmp2/file.txt",
                operation="put",
                create_intermediate_dirs=True,
                dag=dag
            )
    """

    template_fields: Sequence[str] = ("local_filepath", "remote_filepath")

    def __init__(
        self,
        *,
        ftp_conn_id: str = "ftp_default",
        local_filepath: str | list[str],
        remote_filepath: str | list[str],
        operation: str = FTPOperation.PUT,
        create_intermediate_dirs: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ftp_conn_id = ftp_conn_id
        self.operation = operation
        self.create_intermediate_dirs = create_intermediate_dirs
        self.local_filepath = local_filepath
        self.remote_filepath = remote_filepath

    @cached_property
    def hook(self) -> FTPHook:
        """Create and return an FTPHook."""
        return FTPHook(ftp_conn_id=self.ftp_conn_id)

    def execute(self, context: Any) -> str | list[str] | None:
        file_msg = None

        if isinstance(self.local_filepath, str):
            local_filepath_array = [self.local_filepath]
        else:
            local_filepath_array = self.local_filepath

        if isinstance(self.remote_filepath, str):
            remote_filepath_array = [self.remote_filepath]
        else:
            remote_filepath_array = self.remote_filepath

        if len(local_filepath_array) != len(remote_filepath_array):
            raise ValueError(
                f"{len(local_filepath_array)} paths in local_filepath "
                f"!= {len(remote_filepath_array)} paths in remote_filepath"
            )

        if self.operation.lower() not in [FTPOperation.GET, FTPOperation.PUT]:
            raise TypeError(
                f"Unsupported operation value {self.operation}, "
                f"expected {FTPOperation.GET} or {FTPOperation.PUT}."
            )

        for _local_filepath, _remote_filepath in zip(local_filepath_array, remote_filepath_array):
            if self.operation.lower() == FTPOperation.GET:
                local_folder = os.path.dirname(_local_filepath)
                if self.create_intermediate_dirs:
                    Path(local_folder).mkdir(parents=True, exist_ok=True)
                file_msg = f"from {_remote_filepath} to {_local_filepath}"
                self.log.info("Starting to transfer %s", file_msg)
                self.hook.retrieve_file(_remote_filepath, _local_filepath)
            else:
                remote_folder = os.path.dirname(_remote_filepath)
                if self.create_intermediate_dirs:
                    self.hook.create_directory(remote_folder)
                file_msg = f"from {_local_filepath} to {_remote_filepath}"
                self.log.info("Starting to transfer file %s", file_msg)
                self.hook.store_file(_remote_filepath, _local_filepath)

        return self.local_filepath


class FTPSFileTransmitOperator(FTPFileTransmitOperator):
    """
    FTPSFileTransmitOperator for transferring files from remote host to local or vice a versa.
    This operator uses an FTPSHook to open ftps transport channel that serve as basis
    for file transfer.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:FTPSFileTransmitOperator`
    """

    @cached_property
    def hook(self) -> FTPSHook:
        """Create and return an FTPSHook."""
        return FTPSHook(ftp_conn_id=self.ftp_conn_id)
