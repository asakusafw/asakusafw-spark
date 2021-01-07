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

import static com.asakusafw.spark.bootstrap.CoreConstants.*;
import static com.asakusafw.spark.bootstrap.SparkConstants.*;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A bootstrap entry of Asakusa on Spark.
 * @since 0.5.0
 */
public class SparkBootstrap {

    private final Environment environment;

    /**
     * Creates a new instance.
     * @param environment the environment variables
     */
    public SparkBootstrap(Environment environment) {
        this.environment = environment;
    }

    /**
     * Program entry.
     * @param args the program arguments
     * @throws IOException if I/O error was occurred while executing the job
     * @throws InterruptedException if interrupted while executing the job
     */
    public static void main(String... args) throws InterruptedException, IOException {
        SparkBootstrap bootstrap = new SparkBootstrap(Environment.system());
        int exitValue = bootstrap.exec(Context.parse(args));
        if (exitValue != 0) {
            throw new RuntimeException(MessageFormat.format(
                    "Spark execution returned non-zero value: {0}",
                    exitValue));
        }
    }

    /**
     * Executes application.
     * @param context the application context
     * @return the exit value
     * @throws IOException if I/O error was occurred while executing the job
     * @throws InterruptedException if interrupted while executing the job
     */
    public int exec(Context context) throws IOException, InterruptedException {
        Path command = environment.find(ENV_SPARK_LAUNCHER_COMMAND)
                .filter(it -> it.isEmpty() == false)
                .map(Paths::get)
                .orElseThrow(() -> new IllegalStateException(MessageFormat.format(
                        "environmet variable \"{0}\" must be defined",
                        ENV_SPARK_LAUNCHER_COMMAND)));

        Arguments arguments = buildArguments(context);

        ProcessBuilder builder = new ProcessBuilder();
        builder.command(arguments.toCommandLine(command));

        builder.redirectInput(Redirect.INHERIT);
        builder.redirectOutput(Redirect.INHERIT);
        builder.redirectError(Redirect.INHERIT);

        Process process = builder.start();
        try {
            return process.waitFor();
        } finally {
            process.destroy();
        }
    }

    private Classpath buildClasspath(Context context) {
        Classpath cp = new Classpath();

        Path application = getApplication(environment, context.getBatchId());
        cp.add(getAppJobflowLibFile(application, context.getFlowId()), true);
        cp.addEntries(application.resolve(PATH_APP_USER_LIB_DIR), false);

        Path home = getHome(environment);
        cp.addEntries(home.resolve(PATH_SPARK_LIB_DIR), true);
        cp.addEntries(home.resolve(PATH_EXTENSION_LIB_DIR), false);
        cp.addEntries(home.resolve(PATH_CORE_LIB_DIR), true);

        return cp;
    }

    private Arguments buildArguments(Context context) {
        Arguments args = new Arguments();

        Path home = getHome(environment);
        Path application = getApplication(environment, context.getBatchId());

        //--class "$_SPARK_LAUNCHER" \
        args.add("--class", CLASS_SPARK_LAUNCHER);

        //--jars "$_SPARK_LIBJARS" \
        Classpath jars = buildClasspath(context);
        args.add("--jars", jars.getEntries().stream()
                .map(Path::toString)
                .collect(Collectors.joining(",")));

        //--name "$_OPT_BATCH_ID/$_OPT_FLOW_ID/$_OPT_EXECUTION_ID" \
        args.add("--name", String.join("/", context.getBatchId(), context.getFlowId(), context.getExecutionId()));

        //"${_SPARK_FILES[@]}" \
        String files = Stream.of(PATH_CORE_CONF_FILE, PATH_SPARK_CONF_FILE)
                .map(home::resolve)
                .filter(Files::isRegularFile)
                .map(Path::toString)
                .collect(Collectors.joining(","));
        if (files.isEmpty() == false) {
            args.add("--files", files);
        }

        //$ASAKUSA_SPARK_OPTS \
        environment.find(ENV_SPARK_LAUNCHER_OPTIONS).map(SparkBootstrap::split).ifPresent(args::add);

        //"$_SPARK_APP_LIB" \
        args.add(getAppJobflowLibFile(application, context.getFlowId()).toString());

        //--client "$_OPT_CLASS_NAME" \
        args.add("--client", context.getApplicationClassName());

        //--batch-id "$_OPT_BATCH_ID" \
        args.add("--batch-id", context.getBatchId());

        //--flow-id "$_OPT_FLOW_ID" \
        args.add("--flow-id", context.getFlowId());

        //--execution-id "$_OPT_EXECUTION_ID" \
        args.add("--execution-id", context.getExecutionId());

        //--batch-arguments "$_OPT_BATCH_ARGUMENTS," \
        args.add("--batch-arguments", context.getBatchArguments());

        //"${_SPARK_APP_FILES[@]}" \
        // NOTE: we don't use `@file-name|/path/to/filename` because spark-submit does not escape `|` character
        Optional.of(home.resolve(PATH_CORE_CONF_FILE))
                .filter(Files::isRegularFile)
                .ifPresent(it -> args.add("--hadoop-conf", "@" + it));
        Optional.of(home.resolve(PATH_SPARK_CONF_FILE))
                .filter(Files::isRegularFile)
                .ifPresent(it -> args.add("--engine-conf", "@" + it));

        //$ASAKUSA_SPARK_ARGS \
        environment.find(ENV_SPARK_LAUNCHER_ARGUMENTS).map(SparkBootstrap::split).ifPresent(args::add);

        //"$@"
        args.add(context.getExtraArguments());

        return args;
    }

    private static List<String> split(String value) {
        return Arrays.stream(value.split("\\s+"))
                .map(String::trim)
                .filter(s -> s.isEmpty() == false)
                .collect(Collectors.toList());
    }
}
