/*
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.spark.gradle.plugins

import org.gradle.api.Plugin
import org.gradle.api.Project

import com.asakusafw.spark.gradle.plugins.internal.AsakusaSparkCompilerPlugin
import com.asakusafw.spark.gradle.plugins.internal.AsakusaSparkOrganizerPlugin

/**
 * A Gradle plug-in for Asakusa projects for Spark runtime.
 */
class AsakusafwSparkPlugin implements Plugin<Project> {

    /**
     * The Asakusa compiler project version.
     */
    static final String COMPILER_PROJECT_VERSION = '0.1-SNAPSHOT'

    /**
     * The Asakusa Spark project version.
     */
    static final String SPARK_PROJECT_VERSION = '0.1.0-SNAPSHOT'

    /**
     * The target Spark artifact.
     */
    static final String SPARK_ARTIFACT = 'org.apache.spark:spark-core_2.10:1.3.1'

    @Override
    void apply(Project project) {
        project.plugins.withId('asakusafw') {
            project.apply plugin: AsakusaSparkCompilerPlugin
        }
        project.plugins.withId('asakusafw-organizer') {
            project.apply plugin: AsakusaSparkOrganizerPlugin
        }
    }
}
