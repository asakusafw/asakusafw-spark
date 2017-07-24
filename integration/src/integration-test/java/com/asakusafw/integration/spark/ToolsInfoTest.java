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
     * {@code info.sh list plan}.
     */
    @Test
    public void info_list_plan() {
        framework.withLaunch("tools/bin/info.sh", "list", "plan", "-v",
                "spark.perf.average.sort");
    }

    /**
     * {@code info.sh draw plan}.
     */
    @Test
    public void info_draw_plan() {
        framework.withLaunch("tools/bin/info.sh", "draw", "plan", "-v",
                "spark.perf.average.sort");
    }
}
