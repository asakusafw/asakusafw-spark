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

_SPARK_FILES_VALUES=""
_SPARK_FILES=()
_SPARK_APP_FILES=()

if [ -e "$ASAKUSA_HOME/core/conf/asakusa-resources.xml" ]
then
    if [ "$_SPARK_FILES_VALUES" != "" ]
    then
        _SPARK_FILES_VALUES="$_SPARK_FILES_VALUES,"
    fi
    _SPARK_FILES_VALUES="$_SPARK_FILES_VALUES$ASAKUSA_HOME/core/conf/asakusa-resources.xml"
    _SPARK_APP_FILES+=("--hadoop-conf")
    _SPARK_APP_FILES+=("@asakusa-resources.xml|$ASAKUSA_HOME/core/conf/asakusa-resources.xml")
fi

if [ -e "$ASAKUSA_HOME/spark/conf/spark.properties" ]
then
    if [ "$_SPARK_FILES_VALUES" != "" ]
    then
        _SPARK_FILES_VALUES="$_SPARK_FILES_VALUES,"
    fi
    _SPARK_FILES_VALUES="$_SPARK_FILES_VALUES$ASAKUSA_HOME/spark/conf/spark.properties"
    _SPARK_APP_FILES+=("--engine-conf")
    _SPARK_APP_FILES+=("@spark.properties|$ASAKUSA_HOME/spark/conf/spark.properties")
fi

if [ "$_SPARK_FILES_VALUES" != "" ]
then
    _SPARK_FILES+=("--files")
    _SPARK_FILES+=("$_SPARK_FILES_VALUES")
fi

unset _SPARK_FILES_VALUES
