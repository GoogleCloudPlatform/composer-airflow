#!/usr/bin/env python
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
import os
import pathlib
import sys
from pathlib import Path
from typing import List

from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To execute this script, run ./{__file__} [FILE] ..."
    )

ROOT_DIR = pathlib.Path(__file__).resolve().parents[3]


console = Console(color_system="standard", width=200)

errors: List[str] = []

added = False

if __name__ == '__main__':
    for dir, sub_dirs, files in os.walk(str(ROOT_DIR / "tests")):
        for sub_dir in sub_dirs:
            dir_to_check = dir + os.sep + sub_dir
            init_py_path = Path(dir_to_check) / "__init__.py"
            if not init_py_path.exists() and "/test_logs/" not in str(init_py_path):
                init_py_path.touch()
                console.print(f"[yellow] Created {init_py_path}[/]")
                added = True
    sys.exit(1 if added else 0)
