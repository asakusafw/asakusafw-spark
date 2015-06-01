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
package com.asakusafw.spark.gradle.plugins.internal

import static org.junit.Assert.*

import org.gradle.api.Project
import org.gradle.testfixtures.ProjectBuilder
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement

import com.asakusafw.gradle.plugins.AsakusafwPluginConvention
import com.asakusafw.gradle.tasks.AsakusaCompileTask
import com.asakusafw.gradle.tasks.internal.ResolutionUtils
import com.asakusafw.spark.gradle.plugins.AsakusafwCompilerExtension

/**
 * Test for {@link AsakusaSparkCompilerPlugin}.
 */
class AsakusaSparkCompilerPluginTest {

    /**
     * The test initializer.
     */
    @Rule
    public final TestRule initializer = new TestRule() {
        Statement apply(Statement stmt, Description desc) {
            project = ProjectBuilder.builder().withName(desc.methodName).build()
            project.apply plugin: AsakusaSparkCompilerPlugin
            return stmt
        }
    }

    Project project

    /**
     * test for extension.
     */
    @Test
    void extension() {
        AsakusafwPluginConvention root = project.asakusafw
        AsakusafwCompilerExtension extension = root.spark
        assert extension != null

        assert project.file(extension.outputDirectory) == project.file("${project.buildDir}/spark-batchapps")
        assert extension.include == null
        assert extension.exclude == null
        assert extension.customDataModelProcessors.empty
        assert extension.customExternalPortProcessors.empty
        assert extension.customBatchProcessors.empty
        assert extension.customJobflowProcessors.empty
        assert extension.customParticipants.empty

        assert extension.runtimeWorkingDirectory == null

        assert extension.batchIdPrefix == 'spark.'
        assert extension.failOnError == true

        Map<String, String> props = ResolutionUtils.resolveToStringMap(extension.compilerProperties)
        assert props['javac.version'] == root.javac.sourceCompatibility.toString()
        assert props['redirector.rule.com.asakusafw.runtime.core.BatchContext'] == 'com.asakusafw.bridge.api.BatchContext'
        assert props['redirector.rule.com.asakusafw.runtime.core.Report'] == 'com.asakusafw.bridge.api.Report'
        assert props['redirector.rule.com.asakusafw.runtime.directio.api.DirectIo'] == 'com.asakusafw.bridge.directio.api.DirectIo'
    }

    /**
     * test for {@code tasks.sparkCompileBatchapps}.
     */
    @Test
    void tasks_sparkCompileBatchapps() {
        AsakusaCompileTask task = project.tasks.sparkCompileBatchapps
        assert task != null
        assert task.group != null
        assert task.description != null

        AsakusafwPluginConvention root = project.asakusafw
        AsakusafwCompilerExtension spark = root.spark

        root.maxHeapSize = '123m'
        assert task.maxHeapSize == root.maxHeapSize

        assert task.toolClasspath.empty == false

        assert task.explore.empty == false
        assert task.attach.empty == false
        assert task.embed.empty
        assert task.external.empty

        spark.include = 'include.*'
        assert task.include == spark.include

        spark.exclude = 'exclude.*'
        assert task.exclude == spark.exclude

        spark.customDataModelProcessors.add 'custom.DMP'
        assert task.resolvedCustomDataModelProcessors.contains('custom.DMP')

        spark.customExternalPortProcessors.add 'custom.EPP'
        assert task.resolvedCustomExternalPortProcessors.contains('custom.EPP')

        spark.customBatchProcessors.add 'custom.BP'
        assert task.resolvedCustomBatchProcessors.contains('custom.BP')

        spark.customJobflowProcessors.add 'custom.JP'
        assert task.resolvedCustomJobflowProcessors.contains('custom.JP')

        spark.customParticipants.add 'custom.CP'
        assert task.resolvedCustomParticipants.contains('custom.CP')

        spark.runtimeWorkingDirectory = 'RWD'
        assert task.runtimeWorkingDirectory == spark.runtimeWorkingDirectory

        // NOTE: task.compilerProperties will be propagated in 'project.afterEvaluate'
        // spark.compilerProperties.put('TESTING', 'OK')
        // assert task.resolvedCompilerProperties.get('TESTING') == 'OK'

        spark.batchIdPrefix = 'tprefix.'
        assert task.batchIdPrefix == spark.batchIdPrefix

        spark.outputDirectory = 'testing/batchapps'
        assert task.outputDirectory.canonicalFile == project.file(spark.outputDirectory).canonicalFile

        spark.failOnError = false
        assert task.failOnError == spark.failOnError
    }
}
