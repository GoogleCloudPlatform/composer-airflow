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
import re
import shlex
import shutil
from glob import glob
from subprocess import run
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import List

# pylint: disable=no-name-in-module
from docs.exts.docs_build.code_utils import pretty_format_path
from docs.exts.docs_build.errors import DocBuildError, parse_sphinx_warnings
from docs.exts.docs_build.spelling_checks import SpellingError, parse_spelling_warnings
from docs.exts.provider_yaml_utils import load_package_data

# pylint: enable=no-name-in-module

ROOT_PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir, os.pardir)
)
DOCS_DIR = os.path.join(ROOT_PROJECT_DIR, "docs")
ALL_PROVIDER_YAMLS = load_package_data()
AIRFLOW_SITE_DIR = os.environ.get('AIRFLOW_SITE_DIRECTORY')
PROCESS_TIMEOUT = 4 * 60


class AirflowDocsBuilder:
    """Documentation builder for Airflow."""

    def __init__(self, package_name: str, for_production: bool):
        self.package_name = package_name
        self.for_production = for_production

    @property
    def _doctree_dir(self) -> str:
        return f"{DOCS_DIR}/_doctrees/docs/{self.package_name}"

    @property
    def is_versioned(self):
        """Is current documentation package versioned?"""
        # Disable versioning. This documentation does not apply to any issued product and we can update
        # it as needed, i.e. with each new package of providers.
        return self.package_name != 'apache-airflow-providers'

    @property
    def _build_dir(self) -> str:
        if self.is_versioned:
            version = "stable" if self.for_production else "latest"
            return f"{DOCS_DIR}/_build/docs/{self.package_name}/{version}"
        else:
            return f"{DOCS_DIR}/_build/docs/{self.package_name}"

    @property
    def _current_version(self):
        if not self.is_versioned:
            raise Exception("This documentation package is not versioned")
        if self.package_name == 'apache-airflow':
            from airflow.version import version as airflow_version

            return airflow_version
        if self.package_name.startswith('apache-airflow-providers-'):
            provider = next(p for p in ALL_PROVIDER_YAMLS if p['package-name'] == self.package_name)
            return provider['versions'][0]
        return Exception(f"Unsupported package: {self.package_name}")

    @property
    def _publish_dir(self) -> str:
        if self.is_versioned:
            return f"docs-archive/{self.package_name}/{self._current_version}"
        else:
            return f"docs-archive/{self.package_name}"

    @property
    def _src_dir(self) -> str:
        return f"{DOCS_DIR}/{self.package_name}"

    def clean_files(self) -> None:
        """Cleanup all artifacts generated by previous builds."""
        api_dir = os.path.join(self._src_dir, "_api")

        shutil.rmtree(api_dir, ignore_errors=True)
        shutil.rmtree(self._build_dir, ignore_errors=True)
        os.makedirs(api_dir, exist_ok=True)
        os.makedirs(self._build_dir, exist_ok=True)

    def check_spelling(self, verbose):
        """Checks spelling."""
        spelling_errors = []
        with TemporaryDirectory() as tmp_dir, NamedTemporaryFile() as output:
            build_cmd = [
                "sphinx-build",
                "-W",  # turn warnings into errors
                "--color",  # do emit colored output
                "-T",  # show full traceback on exception
                "-b",  # builder to use
                "spelling",
                "-c",
                DOCS_DIR,
                "-d",  # path for the cached environment and doctree files
                self._doctree_dir,
                self._src_dir,  # path to documentation source files
                tmp_dir,
            ]
            print("Executing cmd: ", " ".join([shlex.quote(c) for c in build_cmd]))
            if not verbose:
                print("The output is hidden until an error occurs.")
            env = os.environ.copy()
            env['AIRFLOW_PACKAGE_NAME'] = self.package_name
            if self.for_production:
                env['AIRFLOW_FOR_PRODUCTION'] = 'true'
            completed_proc = run(  # pylint: disable=subprocess-run-check
                build_cmd,
                cwd=self._src_dir,
                env=env,
                stdout=output if not verbose else None,
                stderr=output if not verbose else None,
                timeout=PROCESS_TIMEOUT,
            )
            if completed_proc.returncode != 0:
                output.seek(0)
                print(output.read().decode())

                spelling_errors.append(
                    SpellingError(
                        file_path=None,
                        line_no=None,
                        spelling=None,
                        suggestion=None,
                        context_line=None,
                        message=(
                            f"Sphinx spellcheck returned non-zero exit status: {completed_proc.returncode}."
                        ),
                    )
                )
                warning_text = ""
                for filepath in glob(f"{tmp_dir}/**/*.spelling", recursive=True):
                    with open(filepath) as speeling_file:
                        warning_text += speeling_file.read()

                spelling_errors.extend(parse_spelling_warnings(warning_text, self._src_dir))
        return spelling_errors

    def build_sphinx_docs(self, verbose) -> List[DocBuildError]:
        """Build Sphinx documentation"""
        build_errors = []
        with NamedTemporaryFile() as tmp_file, NamedTemporaryFile() as output:
            build_cmd = [
                "sphinx-build",
                "-T",  # show full traceback on exception
                "--color",  # do emit colored output
                "-b",  # builder to use
                "html",
                "-d",  # path for the cached environment and doctree files
                self._doctree_dir,
                "-c",
                DOCS_DIR,
                "-w",  # write warnings (and errors) to given file
                tmp_file.name,
                self._src_dir,  # path to documentation source files
                self._build_dir,  # path to output directory
            ]
            print("Executing cmd: ", " ".join([shlex.quote(c) for c in build_cmd]))
            if not verbose:
                print("The output is hidden until an error occurs.")

            env = os.environ.copy()
            env['AIRFLOW_PACKAGE_NAME'] = self.package_name
            if self.for_production:
                env['AIRFLOW_FOR_PRODUCTION'] = 'true'

            completed_proc = run(  # pylint: disable=subprocess-run-check
                build_cmd,
                cwd=self._src_dir,
                env=env,
                stdout=output if not verbose else None,
                stderr=output if not verbose else None,
                timeout=PROCESS_TIMEOUT,
            )
            if completed_proc.returncode != 0:
                output.seek(0)
                print(output.read().decode())
                build_errors.append(
                    DocBuildError(
                        file_path=None,
                        line_no=None,
                        message=f"Sphinx returned non-zero exit status: {completed_proc.returncode}.",
                    )
                )
            tmp_file.seek(0)
            warning_text = tmp_file.read().decode()
            # Remove 7-bit C1 ANSI escape sequences
            warning_text = re.sub(r"\x1B[@-_][0-?]*[ -/]*[@-~]", "", warning_text)
            build_errors.extend(parse_sphinx_warnings(warning_text, self._src_dir))
        return build_errors

    def publish(self):
        """Copy documentation packages files to airflow-site repository."""
        print(f"Publishing docs for {self.package_name}")
        output_dir = os.path.join(AIRFLOW_SITE_DIR, self._publish_dir)
        pretty_source = pretty_format_path(self._build_dir, os.getcwd())
        pretty_target = pretty_format_path(output_dir, AIRFLOW_SITE_DIR)
        print(f"Copy directory: {pretty_source} => {pretty_target}")
        if os.path.exists(output_dir):
            if self.is_versioned:
                print(
                    f"Skipping previously existing {output_dir}! "
                    f"Delete it manually if you want to regenerate it!"
                )
                print()
                return
            else:
                shutil.rmtree(output_dir)
        shutil.copytree(self._build_dir, output_dir)
        if self.is_versioned:
            with open(os.path.join(output_dir, "..", "stable.txt"), "w") as stable_file:
                stable_file.write(self._current_version)
        print()


def get_available_providers_packages():
    """Get list of all available providers packages to build."""
    return [provider['package-name'] for provider in ALL_PROVIDER_YAMLS]


def get_available_packages():
    """Get list of all available packages to build."""
    provider_package_names = get_available_providers_packages()
    return ["apache-airflow", *provider_package_names, "apache-airflow-providers"]
