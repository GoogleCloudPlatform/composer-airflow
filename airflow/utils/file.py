# -*- coding: utf-8 -*-
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
from __future__ import absolute_import
from __future__ import unicode_literals
import io
import logging
import os
import re
import zipfile
from typing import Dict, List, Optional, Pattern

import errno
import os
import shutil
from tempfile import mkdtemp

from contextlib import contextmanager


@contextmanager
def TemporaryDirectory(suffix='', prefix=None, dir=None):
    name = mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    try:
        yield name
    finally:
        try:
            shutil.rmtree(name)
        except OSError as e:
            # ENOENT - no such file or directory
            if e.errno != errno.ENOENT:
                raise e


def mkdirs(path, mode):
    """
    Creates the directory specified by path, creating intermediate directories
    as necessary. If directory already exists, this is a no-op.

    :param path: The directory to create
    :type path: str
    :param mode: The mode to give to the directory e.g. 0o755, ignores umask
    :type mode: int
    """
    try:
        o_umask = os.umask(0)
        os.makedirs(path, mode)
    except OSError:
        if not os.path.isdir(path):
            raise
    finally:
        os.umask(o_umask)


ZIP_REGEX = re.compile(r'((.*\.zip){})?(.*)'.format(re.escape(os.sep)))


def correct_maybe_zipped(fileloc):
    """
    If the path contains a folder with a .zip suffix, then
    the folder is treated as a zip archive and path to zip is returned.
    """

    _, archive, _ = ZIP_REGEX.search(fileloc).groups()
    if archive and zipfile.is_zipfile(archive):
        return archive
    else:
        return fileloc


def open_maybe_zipped(fileloc, mode='r'):
    """
    Opens the given file. If the path contains a folder with a .zip suffix, then
    the folder is treated as a zip archive, opening the file inside the archive.

    :return: a file object, as in `open`, or as in `ZipFile.open`.
    """
    _, archive, filename = ZIP_REGEX.search(fileloc).groups()
    if archive and zipfile.is_zipfile(archive):
        return zipfile.ZipFile(archive, mode=mode).open(filename)
    else:
        return io.open(fileloc, mode=mode)
