#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


acquire_rat_jar () {

  URL="http://repo1.maven.org/maven2/org/apache/rat/apache-rat/${RAT_VERSION}/apache-rat-${RAT_VERSION}.jar"

  JAR="$rat_jar"

  # Download rat launch jar if it hasn't been downloaded yet
  if [ ! -f "$JAR" ]; then
    # Download
    printf "Attempting to fetch rat\n"
    JAR_DL="${JAR}.part"
    if [ $(command -v curl) ]; then
      curl -L --silent "${URL}" > "$JAR_DL" && mv "$JAR_DL" "$JAR"
    elif [ $(command -v wget) ]; then
      wget --quiet ${URL} -O "$JAR_DL" && mv "$JAR_DL" "$JAR"
    else
      printf "You do not have curl or wget installed, please install rat manually.\n"
      exit -1
    fi
  fi

  unzip -tq "$JAR" &> /dev/null
  if [ $? -ne 0 ]; then
    # We failed to download
    rm "$JAR"
    printf "Our attempt to download rat locally to ${JAR} failed. Please install rat manually.\n"
    exit -1
  fi
  printf "Done downloading.\n"
}

# Go to the Airflow project root directory
FWDIR="$(cd "`dirname "$0"`"/../..; pwd)"
cd "$FWDIR"

if [ -z "${TRAVIS_CACHE}" ]; then
    TRAVIS_CACHE=/tmp
fi

if test -x "$JAVA_HOME/bin/java"; then
    declare java_cmd="$JAVA_HOME/bin/java"
else
    declare java_cmd=java
fi

export RAT_VERSION=0.11
export rat_jar="${TRAVIS_CACHE}"/lib/apache-rat-${RAT_VERSION}.jar
mkdir -p ${TRAVIS_CACHE}/lib


[[ -f "$rat_jar" ]] || acquire_rat_jar || {
    echo "Download failed. Obtain the rat jar manually and place it at $rat_jar"
    exit 1
}

echo "Running license checks. This can take a while."
$java_cmd -jar "$rat_jar" -E "$FWDIR"/.rat-excludes  -d "$FWDIR" > rat-results.txt

if [ $? -ne 0 ]; then
   echo "RAT exited abnormally"
   exit 1
fi

ERRORS="$(cat rat-results.txt | grep -e "??")"

if test ! -z "$ERRORS"; then
    echo "Could not find Apache license headers in the following files:"
    echo "$ERRORS"
    COUNT=`echo "${ERRORS}" | wc -l`
    # due to old builds can be removed later
    rm -rf ${TRAVIS_CACHE}/rat-error-count
    if [ ! -f ${TRAVIS_CACHE}/rat-error-count-builds ]; then
        [ "${TRAVIS_PULL_REQUEST}" = "false" ] && echo ${COUNT} > ${TRAVIS_CACHE}/rat-error-count-builds
        OLD_COUNT=${COUNT}
    else
        typeset -i OLD_COUNT=$(cat ${TRAVIS_CACHE}/rat-error-count-builds)
    fi
    if [ ${COUNT} -gt ${OLD_COUNT} ]; then
        echo "New missing licenses (${COUNT} vs ${OLD_COUNT}) detected. Please correct them by adding them to to header of your files"
        exit 1
    else
        [ "${TRAVIS_PULL_REQUEST}" = "false" ] && echo ${COUNT} > ${TRAVIS_CACHE}/rat-error-count-builds
    fi
    exit 0
else
    echo -e "RAT checks passed."
fi