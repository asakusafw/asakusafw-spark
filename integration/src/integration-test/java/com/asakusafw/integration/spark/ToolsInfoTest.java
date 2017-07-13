/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.integration.spark;

import static com.asakusafw.integration.spark.Util.*;
import static org.hamcrest.Matchers.*;

import java.util.Optional;

import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;

import com.asakusafw.integration.AsakusaConfigurator;
import com.asakusafw.integration.AsakusaProjectProvider;
import com.asakusafw.utils.gradle.Bundle;
import com.asakusafw.utils.gradle.ContentsConfigurator;

/**
 * Test for {@code tools/bin/info.sh}.
 */
public class ToolsInfoTest {

    /**
     * project provider.
     */
    @ClassRule
    public static final AsakusaProjectProvider PROVIDER = new AsakusaProjectProvider()
            .withProject(ContentsConfigurator.copy(data("spark")))
            .withProject(ContentsConfigurator.copy(data("ksv")))
            .withProject(ContentsConfigurator.copy(data("logback-test")))
            .withProject(AsakusaConfigurator.projectHome())
            .withProject(AsakusaConfigurator.hadoop(AsakusaConfigurator.Action.UNSET_ALWAYS))
            .withProvider(provider -> {
                // install framework only once
                framework = provider.newInstance("inf")
                        .gradle("attachSparkBatchapps", "installAsakusafw")
                        .getFramework();
            });

    static Bundle framework;

    /**
     * {@code info.sh}.
     */
    @Test
    public void info() {
        framework.withLaunch("tools/bin/info.sh");
    }

    /**
     * {@code info.sh list batch}.
     */
    @Test
    public void info_list_batch() {
        framework.withLaunch("tools/bin/info.sh", "list", "batch", "-v");
    }

    /**
     * {@code info.sh list batch}.
     */
    @Test
    public void info_list_jobflow() {
        checkInfo();
        framework.withLaunch("tools/bin/info.sh", "list", "jobflow", "-v",
                "spark.perf.average.sort");
    }

    /**
     * {@code info.sh list batch}.
     */
    @Test
    public void info_list_plan() {
        checkInfo();
        framework.withLaunch("tools/bin/info.sh", "list", "plan", "-v",
                "spark.perf.average.sort");
    }

    /**
     * {@code info.sh list operator}.
     */
    @Test
    public void info_list_operator() {
        checkInfo();
        framework.withLaunch("tools/bin/info.sh", "list", "operator", "-v",
                "spark.perf.average.sort");
    }

    /**
     * {@code info.sh list directio-*}.
     */
    @Test
    public void info_list_directio() {
        checkInfo();
        framework.withLaunch("tools/bin/info.sh", "list", "directio-input", "-v",
                "spark.perf.average.sort");
    }

    /**
     * {@code info.sh list windgate-*}.
     */
    @Test
    public void info_list_windgate() {
        checkInfo();
        framework.withLaunch("tools/bin/info.sh", "list", "windgate-output", "-v",
                "spark.wg.perf.average.sort");
    }

    /**
     * {@code info.sh draw batch}.
     */
    @Test
    public void info_draw_jobflow() {
        checkInfo();
        framework.withLaunch("tools/bin/info.sh", "draw", "jobflow", "-a",
                "spark.perf.average.sort");
    }

    /**
     * {@code info.sh draw batch}.
     */
    @Test
    public void info_draw_plan() {
        checkInfo();
        framework.withLaunch("tools/bin/info.sh", "draw", "plan", "-a",
                "spark.perf.average.sort");
    }

    /**
     * {@code info.sh draw operator}.
     */
    @Test
    public void info_draw_operator() {
        checkInfo();
        framework.withLaunch("tools/bin/info.sh", "draw", "operator", "-a",
                "spark.perf.average.sort");
    }

    private static void checkInfo() {
        Assume.assumeThat(
                framework.find("batchapps/spark.perf.average.sort/etc/batch-info.json"),
                is(not(Optional.empty())));
    }
}
