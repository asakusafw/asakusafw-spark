/**
 * Copyright 2011-2021 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.bootstrap;

/**
 * Constants of Asakusa on Spark.
 * @since 0.5.0
 */
public final class SparkConstants {


    static final String PATH_SPARK_BASE = "spark";

    /**
     * The path of the Asakusa on Spark configuration directory.
     */
    public static final String PATH_SPARK_CONF_DIR = PATH_SPARK_BASE + "/conf";

    /**
     * The path of the Asakusa on Spark engine configuration file.
     */
    public static final String PATH_SPARK_CONF_FILE = PATH_SPARK_CONF_DIR + "/spark.properties";

    /**
     * The path of the Asakusa on Spark bridge libraries directory.
     */
    public static final String PATH_SPARK_LIB_DIR = PATH_SPARK_BASE + "/lib";

    /**
     * The class name of Asakusa on Spark launcher class.
     */
    public static final String CLASS_SPARK_LAUNCHER = "com.asakusafw.spark.runtime.Launcher";

    /**
     * The environment variable name of Spark launcher command.
     */
    public static final String ENV_SPARK_LAUNCHER_COMMAND = "SPARK_CMD";

    /**
     * The environment variable name of Hadoop home directory.
     */
    public static final String ENV_HADOOP_HOME = "HADOOP_HOME";

    /**
     * The environment variable name of extra Asakusa on Spark launcher options.
     */
    public static final String ENV_SPARK_LAUNCHER_OPTIONS = "ASAKUSA_SPARK_OPTS";

    /**
     * The environment variable name of extra Asakusa on Spark launcher arguments.
     */
    public static final String ENV_SPARK_LAUNCHER_ARGUMENTS = "ASAKUSA_SPARK_ARGS";

    private SparkConstants() {
        return;
    }
}
