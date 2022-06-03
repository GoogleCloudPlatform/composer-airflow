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
import re
import sys
from pathlib import Path
from typing import List

from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )


console = Console(color_system="standard", width=200)

errors: List[str] = []

GET_ATTR_MATCHER = re.compile(r".*getattr\((ti|TI), ['\"]run_id['\"]\).*")
TI_RUN_ID_MATCHER = re.compile(r".*(ti|TI)\.run_id.*")
TRY_NUM_MATCHER = re.compile(r".*context.*\[[\"']try_number[\"']].*")


def _check_file(_file: Path):
    lines = _file.read_text().splitlines()

    for index, line in enumerate(lines):
        if GET_ATTR_MATCHER.match(line) or TI_RUN_ID_MATCHER.match(line):
            errors.append(
                f"[red]In {_file}:{index} there is a forbidden construct "
                f"(Airflow 2.2+ only):[/]\n\n"
                f"{lines[index]}\n\n"
                f"[yellow]You should not retrieve run_id from Task Instance in providers as it "
                f"is not available in Airflow 2.1[/]\n\n"
                f"Use one of: \n\n"
                f"     context['run_id']\n\n"
                f"     getattr(ti, 'run_id', '<DEFAULT>')\n\n"
            )
        if "XCom.get_value(" in line:
            if "if ti_key is not None:" not in lines[index - 1]:
                errors.append(
                    f"[red]In {_file}:{index} there is a forbidden construct "
                    f"(Airflow 2.3.0 only):[/]\n\n"
                    f"{lines[index-1]}\n{lines[index]}\n\n"
                    f"[yellow]When you use XCom.get_value( in providers, it should be in the form:[/]\n\n"
                    f"if ti_key is not None:\n"
                    f"    value = XCom.get_value(...., ti_key=ti_key)\n\n"
                    f"See: https://airflow.apache.org/docs/apache-airflow-providers/"
                    f"howto/create-update-providers.html#using-providers-with-dynamic-task-mapping\n"
                )
        if "timezone.coerce_datetime" in line:
            errors.append(
                f"[red]In {_file}:{index} there is a forbidden construct "
                f"(Airflow 2.2+ only):[/]\n\n"
                f"{lines[index]}\n\n"
                f"[yellow]You should not use coerce_datetime in providers "
                f"as it is not available in Airflow 2.1[/]"
            )
        if "ti.map_index" in line:
            errors.append(
                f"[red]In {_file}:{index} there is a forbidden construct "
                f"(Airflow 2.3+ only):[/]\n\n"
                f"{lines[index]}\n\n"
                f"[yellow]You should not use map_index field in providers "
                f"as it is not available in Airflow 2.2[/]"
            )

        if TRY_NUM_MATCHER.match(line):
            errors.append(
                f"[red]In {_file}:{index} there is a forbidden construct "
                f"(Airflow 2.3+ only):[/]\n\n"
                f"{lines[index]}\n\n"
                f"[yellow]You should not expect try_number field for context in providers "
                f"as it is not available in Airflow 2.2[/]"
            )


if __name__ == '__main__':
    for file in sys.argv[1:]:
        _check_file(Path(file))
    if errors:
        console.print("[red]Found Airflow 2.1 compatibility problems in providers:[/]\n")
        for error in errors:
            console.print(f"{error}")
        sys.exit(1)
