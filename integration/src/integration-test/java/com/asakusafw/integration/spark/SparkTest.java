/**
 * Copyright 2011-2019 Asakusa Framework Team.
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.integration.AsakusaConfigurator;
import com.asakusafw.integration.AsakusaConstants;
import com.asakusafw.integration.AsakusaProject;
import com.asakusafw.integration.AsakusaProjectProvider;
import com.asakusafw.utils.gradle.Bundle;
import com.asakusafw.utils.gradle.ContentsConfigurator;
import com.asakusafw.utils.gradle.PropertyConfigurator;

/**
 * Test for {@code spark}.
 */
public class SparkTest {

    /**
     * project provider.
     */
    @Rule
    public final AsakusaProjectProvider provider = new AsakusaProjectProvider()
            .withProject(ContentsConfigurator.copy(data("spark")))
            .withProject(ContentsConfigurator.copy(data("ksv")))
            .withProject(ContentsConfigurator.copy(data("logback-test")))
            .withProject(PropertyConfigurator.of("hive.version", (String) null))
            .withProject(AsakusaConfigurator.projectHome())
            .withProject(AsakusaConfigurator.hadoop(AsakusaConfigurator.Action.UNSET_IF_UNDEFINED))
            .withProject(AsakusaConfigurator.spark(AsakusaConfigurator.Action.SKIP_IF_UNDEFINED));

    /**
     * help.
     */
    @Test
    public void help() {
        AsakusaProject project = provider.newInstance("prj");
        project.gradle("help");
    }

    /**
     * version.
     */
    @Test
    public void version() {
        AsakusaProject project = provider.newInstance("prj");
        project.gradle("asakusaVersions");
    }

    /**
     * upgrade.
     */
    @Test
    public void upgrade() {
        AsakusaProject project = provider.newInstance("prj");
        project.gradle("asakusaUpgrade");
        Bundle contents = project.getContents();
        assertThat(contents.find("gradlew"), is(not(Optional.empty())));
        assertThat(contents.find("gradlew.bat"), is(not(Optional.empty())));
    }

    /**
     * {@code assemble}.
     */
    @Test
    public void assemble() {
        AsakusaProject project = provider.newInstance("prj");
        project.gradle("assemble");
        Bundle contents = project.getContents();
        assertThat(contents.find("build/asakusafw-prj.tar.gz"), is(not(Optional.empty())));
    }

    /**
     * {@code installAsakusafw}.
     */
    @Test
    public void installAsakusafw() {
        AsakusaProject project = provider.newInstance("prj");
        project.gradle("installAsakusafw");
        Bundle framework = project.getFramework();
        assertThat(framework.find("spark"), is(not(Optional.empty())));
    }

    /**
     * {@code test} w/ {@code HADOOP_CMD}.
     */
    @Test
    public void test_hadoop() {
        AsakusaProject project = provider.newInstance("prj")
                .with(AsakusaConfigurator.hadoop(AsakusaConfigurator.Action.SKIP_IF_UNDEFINED));
        project.gradle("installAsakusafw", "test");
    }

    /**
     * {@code test} w/o {@code HADOOP_CMD}.
     */
    @Test
    public void test_nohadoop() {
        AsakusaProject project = provider.newInstance("prj")
                .with(AsakusaConfigurator.hadoop(AsakusaConfigurator.Action.UNSET_ALWAYS));
        project.gradle("installAsakusafw", "test");
    }

    /**
     * YAESS.
     */
    @Test
    public void yaess() {
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
                AsakusaConstants.CMD_YAESS, "spark.perf.average.sort",
                "-A", "input=input", "-A", "output=output");

        project.getContents().get("var/data/output", dir -> {
            List<String> results = Files.list(dir)
                .flatMap(Util::lines)
                .sorted()
                .collect(Collectors.toList());
            assertThat(results, containsInAnyOrder(csv));
        });
    }

    /**
     * YAESS w/ WindGate tasks.
     */
    @Test
    public void yaess_windgate() {
        AsakusaProject project = provider.newInstance("prj");

        project.gradle("attachSparkBatchapps", "installAsakusafw");

        String[] csv = new String[] {
                "1,1.0,A",
                "2,2.0,B",
                "3,3.0,C",
        };

        project.getContents().put("var/windgate/input.csv", f -> {
            Files.write(f, Arrays.asList(csv), StandardCharsets.UTF_8);
        });

        project.getFramework().withLaunch(
                AsakusaConstants.CMD_YAESS, "spark.wg.perf.average.sort",
                "-A", "input=input.csv", "-A", "output=output.csv");

        project.getContents().get("var/windgate/output.csv", file -> {
            List<String> results = lines(file)
                .collect(Collectors.toList());
            assertThat(results, containsInAnyOrder(csv));
        });
    }

    /**
     * {@code run}.
     */
    @Test
    public void run() {
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
                AsakusaConstants.CMD_PORTAL, "run", "spark.perf.average.sort",
                "-Ainput=input", "-Aoutput=output");

        project.getContents().get("var/data/output", dir -> {
            List<String> results = Files.list(dir)
                .flatMap(Util::lines)
                .sorted()
                .collect(Collectors.toList());
            assertThat(results, containsInAnyOrder(csv));
        });
    }

    /**
     * {@code run} w/ WindGate task.
     */
    @Test
    public void run_windgate() {
        AsakusaProject project = provider.newInstance("prj");

        project.gradle("attachSparkBatchapps", "installAsakusafw");

        String[] csv = new String[] {
                "1,1.0,A",
                "2,2.0,B",
                "3,3.0,C",
        };

        project.getContents().put("var/windgate/input.csv", f -> {
            Files.write(f, Arrays.asList(csv), StandardCharsets.UTF_8);
        });

        project.getFramework().withLaunch(
                AsakusaConstants.CMD_PORTAL, "run", "spark.wg.perf.average.sort",
                "-Ainput=input.csv", "-Aoutput=output.csv");

        project.getContents().get("var/windgate/output.csv", file -> {
            List<String> results = lines(file)
                .collect(Collectors.toList());
            assertThat(results, containsInAnyOrder(csv));
        });
    }
}
