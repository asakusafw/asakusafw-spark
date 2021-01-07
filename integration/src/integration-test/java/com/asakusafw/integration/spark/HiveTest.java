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
package com.asakusafw.integration.spark;

import static com.asakusafw.integration.spark.Util.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.asakusafw.integration.AsakusaConfigurator;
import com.asakusafw.integration.AsakusaConstants;
import com.asakusafw.integration.AsakusaProject;
import com.asakusafw.integration.AsakusaProjectProvider;
import com.asakusafw.utils.gradle.ContentsConfigurator;
import com.asakusafw.utils.gradle.PropertyConfigurator;

/**
 * Test for Direct I/O Hive.
 */
@RunWith(Parameterized.class)
public class HiveTest {

    /**
     * Return the test parameters.
     * @return the test parameters
     */
    @Parameters(name = "{0}")
    public static Object[][] getTestParameters() {
        return new Object[][] {
            { "default", },
            {   "1.1.1", },
            {   "1.2.1", },
            {   "1.2.2", },
            {   "2.3.4", },
        };
    }

     /**
     * project provider.
     */
    @Rule
    public final AsakusaProjectProvider provider = new AsakusaProjectProvider()
            .withProject(ContentsConfigurator.copy(data("spark")))
            .withProject(ContentsConfigurator.copy(data("ksv-hive")))
            .withProject(ContentsConfigurator.copy(data("logback-test")))
            .withProject(AsakusaConfigurator.projectHome())
            .withProject(AsakusaConfigurator.hadoop(AsakusaConfigurator.Action.UNSET_IF_UNDEFINED))
            .withProject(AsakusaConfigurator.spark(AsakusaConfigurator.Action.SKIP_IF_UNDEFINED));

    /**
     * creates a new instance.
     * @param hiveVersion the hive-exec version
     */
    public HiveTest(String hiveVersion) {
        provider.withProject(PropertyConfigurator.of("hive.version", String.valueOf(hiveVersion)));
    }

    /**
     * {@code run}.
     */
    @Test
    public void run_orc() {
        AsakusaProject project = provider.newInstance("prj");
        project.gradle("attachSparkBatchapps", "installAsakusafw");

        String[] csv = new String[] {
                "1,1.0,A",
                "2,2.0,B",
                "3,3.0,C",
        };
        project.getContents().put("var/data/input/file.csv", f -> {
            Files.write(f, Arrays.asList(csv), StandardCharsets.UTF_8);
        });

        project.getFramework().withLaunch(
                AsakusaConstants.CMD_PORTAL, "run", "spark.perf.orc.io",
                "-Acsv.input=input", "-Acsv.output=output",
                "-Ainput=tmp", "-Aoutput=tmp");

        project.getContents().get("var/data/output", dir -> {
            List<String> results = Files.list(dir)
                .flatMap(Util::lines)
                .sorted()
                .map(this::normalize)
                .collect(Collectors.toList());
            assertThat(results, containsInAnyOrder(csv));
        });
    }

    /**
     * {@code run}.
     */
    @Test
    public void run_parquet() {
        AsakusaProject project = provider.newInstance("prj");
        project.gradle("attachSparkBatchapps", "installAsakusafw");

        String[] csv = new String[] {
                "1,1.0,A",
                "2,2.0,B",
                "3,3.0,C",
        };
        project.getContents().put("var/data/input/file.csv", f -> {
            Files.write(f, Arrays.asList(csv), StandardCharsets.UTF_8);
        });

        project.getFramework().withLaunch(
                AsakusaConstants.CMD_PORTAL, "run", "spark.perf.parquet.io",
                "-Acsv.input=input", "-Acsv.output=output",
                "-Ainput=tmp", "-Aoutput=tmp");

        project.getContents().get("var/data/output", dir -> {
            List<String> results = Files.list(dir)
                .flatMap(Util::lines)
                .sorted()
                .map(this::normalize)
                .collect(Collectors.toList());
            assertThat(results, containsInAnyOrder(csv));
        });
    }

    private String normalize(String line) {
        String[] segments = line.split(",");
        assertThat(segments.length, is(3));
        return String.format("%d,%s,%s",
                Integer.parseInt(segments[0]),
                new BigDecimal(segments[1]).setScale(1).toPlainString(),
                segments[2]);
    }
}
