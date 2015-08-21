#!/bin/bash
#
# Copyright 2011-2015 Asakusa Framework Team.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


usage() {
    cat 1>&2 <<EOF
Asakusa on Spark Command Line

Usage:
    $0 batch-id flow-id execution-id batch-arguments class-name [direct-arguments...]

Parameters:
    batch-id
        batch ID of current execution
    flow-id
        flow ID of current execution
    execution-id
        execution ID of current execution
    batch-arguments
        The arguments for this execution
        This must be form of "key1=value1,key2=value2,...",
        and the special characters '=', ',', '\' can be escaped by '\'.
    class-name
        Fully qualified class name of program entry
    direct-arguments...
        Direct arguments for Spark launcher
EOF
}

import() {
    _SCRIPT="$1"
    if [ -e "$_SCRIPT" ]
    then
        . "$_SCRIPT"
    else
        echo "$_SCRIPT is not found" 1>&2
        exit 1
    fi
}

if [ $# -lt 5 ]
then
    echo "$@" 1>&2
    usage
    exit 1
fi

_OPT_BATCH_ID="$1"
shift
_OPT_FLOW_ID="$1"
shift
_OPT_EXECUTION_ID="$1"
shift
_OPT_BATCH_ARGUMENTS="$1"
shift
_OPT_CLASS_NAME="$1"
shift

_SPARK_ROOT="$(cd "$(dirname "$0")/.." ; pwd)"
import "$_SPARK_ROOT/conf/env.sh"
import "$_SPARK_ROOT/libexec/validate-env.sh"

# Move to home directory
cd

_SPARK_LAUNCHER="com.asakusafw.spark.runtime.Launcher"
_SPARK_APP_LIB="$ASAKUSA_BATCHAPPS_HOME/$_OPT_BATCH_ID/lib/jobflow-${_OPT_FLOW_ID}.jar"

import "$_SPARK_ROOT/libexec/configure-spark-cmd.sh"
import "$_SPARK_ROOT/libexec/configure-libjars.sh"
import "$_SPARK_ROOT/libexec/configure-files.sh"

echo "Starting Asakusa Spark:"
echo "          Spark Command: $SPARK_CMD"
echo "            App Library: $_SPARK_APP_LIB"
echo "               Batch ID: $_OPT_BATCH_ID"
echo "                Flow ID: $_OPT_FLOW_ID"
echo "           Execution ID: $_OPT_EXECUTION_ID"
echo "                  Class: $_OPT_CLASS_NAME"
echo "     ASAKUSA_SPARK_OPTS: $ASAKUSA_SPARK_OPTS"
echo " ASAKUSA_SPARK_APP_CONF: $ASAKUSA_SPARK_APP_CONF"
echo "            SPARK_FILES: ${_SPARK_FILES[@]}"
echo "        SPARK_APP_FILES: ${_SPARK_APP_FILES[@]}"

export ASAKUSA_SPARK_APP_CONF

_SPARK_EXEC=()

if [ "$SPARK_CMD_LAUNCHER" != "" ]
then
    _SPARK_EXEC+=($SPARK_CMD_LAUNCHER)
fi

_SPARK_EXEC+=("$SPARK_CMD")

"${_SPARK_EXEC[@]}" \
    --class "$_SPARK_LAUNCHER" \
    --jars "$_SPARK_LIBJARS" \
    --name "$_OPT_BATCH_ID/$_OPT_FLOW_ID/$_OPT_EXECUTION_ID" \
    "${_SPARK_FILES[@]}" \
    $ASAKUSA_SPARK_OPTS \
    "$_SPARK_APP_LIB" \
    --client "$_OPT_CLASS_NAME" \
    --batch-id "$_OPT_BATCH_ID" \
    --flow-id "$_OPT_FLOW_ID" \
    --execution-id "$_OPT_EXECUTION_ID" \
    --batch-arguments "$_OPT_BATCH_ARGUMENTS," \
    "${_SPARK_APP_FILES[@]}" \
    $ASAKUSA_SPARK_APP_CONF \
    "$@"

_SPARK_RET=$?
if [ $_SPARK_RET -ne 0 ]
then
    echo "Spark failed with exit code: $_SPARK_RET" 1>&2
    echo "  Runtime Lib: $_SPARK_APP_LIB"  1>&2
    echo "     Launcher: $_SPARK_LAUNCHER"  1>&2
    echo "  Stage Class: $_OPT_CLASS_NAME" 1>&2
    echo "     Batch ID: $_OPT_BATCH_ID" 1>&2
    echo "      Flow ID: $_OPT_FLOW_ID" 1>&2
    echo " Execution ID: $_OPT_EXECUTION_ID" 1>&2
    echo "   Batch Args: $_OPT_BATCH_ARGUMENTS" 1>&2
    echo "    Libraries: --jars $_SPARK_LIBJARS"  1>&2
    echo "  Extra Props: $@" 1>&2
    exit $_SPARK_RET
fi
