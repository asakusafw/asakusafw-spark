/*
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.spark.gradle.plugins.internal

import org.gradle.api.Project
import org.gradle.testfixtures.ProjectBuilder
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement

import com.asakusafw.gradle.plugins.AsakusafwOrganizerPluginConvention
import com.asakusafw.spark.gradle.plugins.AsakusafwOrganizerSparkExtension

/**
 * Test for {@link AsakusaSparkOrganizerPlugin}.
 */
class AsakusaSparkOrganizerPluginTest {

    /**
     * The test initializer.
     */
    @Rule
    public final TestRule initializer = new TestRule() {
        Statement apply(Statement stmt, Description desc) {
            project = ProjectBuilder.builder().withName(desc.methodName).build()
            project.apply plugin: AsakusaSparkOrganizerPlugin
            return stmt
        }
    }

    Project project

    /**
     * test for base plug-ins.
     */
    @Test
    void base() {
        assert project.plugins.hasPlugin('asakusafw-organizer') != null
        assert project.plugins.hasPlugin(AsakusaSparkBasePlugin) != null
    }

    /**
     * test for extension.
     */
    @Test
    void extension() {
        AsakusafwOrganizerPluginConvention root = project.asakusafwOrganizer
        AsakusafwOrganizerSparkExtension extension = root.spark
        assert extension != null

        assert extension.enabled == true

        assert root.profiles.dev.spark.enabled == true
        assert root.profiles.prod.spark.enabled == true

        root.profiles.testing {
            // ok
        }
        assert root.profiles.testing.spark.enabled == true
    }

    /**
     * test for extension.
     */
    @Test
    void extension_override_yaess_iterative() {
        AsakusafwOrganizerPluginConvention root = project.asakusafwOrganizer
        assert root.yaess.iterativeEnabled == true
        assert root.profiles.dev.yaess.iterativeEnabled == true
        assert root.profiles.prod.yaess.iterativeEnabled == true

        root.profiles.testing {
            // ok
        }
        assert root.profiles.testing.yaess.iterativeEnabled == true
    }

    /**
     * test for extension.
     */
    @Test
    void extension_inherited() {
        AsakusafwOrganizerPluginConvention root = project.asakusafwOrganizer
        AsakusafwOrganizerSparkExtension extension = root.spark

        extension.enabled = false

        assert root.profiles.dev.spark.enabled == false
        assert root.profiles.prod.spark.enabled == false

        root.profiles.prod.spark.enabled = true
        assert extension.enabled == false
        assert root.profiles.dev.spark.enabled == false
        assert root.profiles.prod.spark.enabled == true


        root.profiles.testing {
            // ok
        }
        assert root.profiles.testing.spark.enabled == false
    }

    /**
     * test for {@code tasks.attachComponentSpark_*}.
     */
    @Test
    void tasks_attachComponentSpark() {
        assert project.tasks.findByName('attachComponentSpark_dev') != null
        assert project.tasks.findByName('attachComponentSpark_prod') != null

        assert project.tasks.findByName('attachComponentSpark_testing') == null
        project.asakusafwOrganizer.profiles.testing {
            // ok
        }
        assert project.tasks.findByName('attachComponentSpark_testing') != null
    }
}
