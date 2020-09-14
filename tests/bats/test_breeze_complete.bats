#!/usr/bin/env bats

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

# shellcheck disable=SC2030,SC2031

@test "Test get_known_values short" {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"

  breeze_complete::get_known_values_breeze "-p"
  assert_equal "${_breeze_known_values}" "2.7 3.5 3.6 3.7 3.8"
}

@test "Test get_known_values long" {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"

  breeze_complete::get_known_values_breeze "--python"
  assert_equal "${_breeze_known_values}" "2.7 3.5 3.6 3.7 3.8"
}

@test "Test wrong get_known_values" {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"

  breeze_complete::get_known_values_breeze "unknown"
  assert_equal "${_breeze_known_values}" ""
}

@test "Test build options for breeze short" {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"

  run echo "${_breeze_getopt_short_options}"
  assert_output --regexp '^([a-zA-Z]:?,)*[a-zA-Z]:?$'
}

@test "Test build options for breeze long" {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"

  run echo "${_breeze_getopt_long_options}"
  assert_output --regexp '^([a-zA-Z\-]+:?,)*[a-zA-Z\-]+:?$'
}

@test "Test listcontains matches" {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"

  run breeze_complete::_listcontains_breeze "word1 word2 word3" "word2"
  assert_success
}

@test "Test listcontains does not match" {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"

  run breeze_complete::_listcontains_breeze "word1 word2 word3" "word4"
  assert_failure
}

@test "Test convert options short" {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"
  all_options=""
  options_with_extra_arguments=""
  breeze_complete::_convert_options "-" "a b c: d"

  assert_equal "${all_options}" " -a -b -c -d"
  assert_equal "${options_with_extra_arguments}" " -c"
}

@test "Test convert options long" {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"
  all_options=""
  options_with_extra_arguments=""
  breeze_complete::_convert_options "--" "longa longb longc: longd:"

  assert_equal "${all_options}" " --longa --longb --longc --longd"
  assert_equal "${options_with_extra_arguments}" " --longc --longd"
}

@test "Test autocomplete --pyt" {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"
  COMP_CWORD=0
  COMP_WORDS=("--pyt")
  breeze_complete::_comp_breeze

  assert_equal "${COMPREPLY[*]}" "--python"
}

@test "Test autocomplete --python " {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"
  COMP_CWORD=1
  COMP_WORDS=("--python" "")
  breeze_complete::_comp_breeze

  assert_equal "${COMPREPLY[*]}" "2.7 3.5 3.6 3.7 3.8"
}

@test "Test autocomplete --python with prefix" {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"
  COMP_CWORD=1
  COMP_WORDS=("--python" "3")
  breeze_complete::_comp_breeze

  assert_equal "${COMPREPLY[*]}" "3.5 3.6 3.7 3.8"
}

@test "Test autocomplete build-" {
  load bats_utils
  #shellcheck source=breeze-complete
  source "${AIRFLOW_SOURCES}/breeze-complete"
  COMP_CWORD=0
  COMP_WORDS=("build-")
  breeze_complete::_comp_breeze

  assert_equal "${COMPREPLY[*]}" "build-docs build-image"
}
