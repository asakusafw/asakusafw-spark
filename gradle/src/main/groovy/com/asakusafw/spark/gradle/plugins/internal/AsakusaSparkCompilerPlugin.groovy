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

import static com.asakusafw.spark.gradle.plugins.AsakusafwSparkPlugin.*

import org.gradle.api.Plugin
import org.gradle.api.Project

import com.asakusafw.gradle.plugins.AsakusafwPlugin
import com.asakusafw.gradle.plugins.AsakusafwPluginConvention
import com.asakusafw.gradle.tasks.AsakusaCompileTask
import com.asakusafw.gradle.tasks.internal.ResolutionUtils
import com.asakusafw.spark.gradle.plugins.AsakusafwCompilerExtension

/**
 * A Gradle sub plug-in for Asakusa on Spark compiler.
 */
class AsakusaSparkCompilerPlugin implements Plugin<Project> {

    private static final Map<String, String> REDIRECT = [
            'com.asakusafw.runtime.core.BatchContext' : 'com.asakusafw.bridge.api.BatchContext',
            'com.asakusafw.runtime.core.Report' : 'com.asakusafw.bridge.api.Report',
            'com.asakusafw.runtime.directio.api.DirectIo' : 'com.asakusafw.bridge.directio.api.DirectIo',
    ]

    private Project project

    @Override
    void apply(Project project) {
        this.project = project

        // may be no effects
        project.apply plugin: 'asakusafw'

        configureConvention()
        configureConfigurations()
        defineTasks()
    }

    private void configureConvention() {
        AsakusafwPluginConvention convention = project.asakusafw
        AsakusafwCompilerExtension spark = convention.extensions.create('spark', AsakusafwCompilerExtension)
        spark.conventionMapping.with {
            outputDirectory = { project.relativePath(new File(project.buildDir, 'spark-batchapps')) }
            batchIdPrefix = { (String) 'spark.' }
            failOnError = { true }
        }
        REDIRECT.each { k, v ->
            spark.compilerProperties.put((String) "redirector.rule.${k}", v)
        }
        spark.compilerProperties.put('javac.version', { convention.javac.sourceCompatibility.toString() })
    }

    private void configureConfigurations() {
        project.configurations {
            asakusaSparkCompiler {
                description 'Asakusa DSL Compiler for Spark environment'
                extendsFrom project.configurations.compile
            }
        }
        project.afterEvaluate {
            project.dependencies {
                asakusaSparkCompiler "com.asakusafw.spark:asakusa-spark-compiler-core:${SPARK_PROJECT_VERSION}"
                asakusaSparkCompiler SPARK_ARTIFACT

                asakusaSparkCompiler "com.asakusafw.lang.compiler:asakusa-compiler-cli:${COMPILER_PROJECT_VERSION}"
                asakusaSparkCompiler "com.asakusafw:asakusa-dsl-vocabulary:${project.asakusafw.asakusafwVersion}"
                asakusaSparkCompiler "com.asakusafw:simple-graph:${project.asakusafw.asakusafwVersion}"

                asakusaSparkCompiler "com.asakusafw.lang.compiler:asakusa-compiler-extension-cleanup:${COMPILER_PROJECT_VERSION}"
                asakusaSparkCompiler "com.asakusafw.lang.compiler:asakusa-compiler-extension-redirector:${COMPILER_PROJECT_VERSION}"
                asakusaSparkCompiler "com.asakusafw.lang.compiler:asakusa-compiler-extension-yaess:${COMPILER_PROJECT_VERSION}"

                asakusaSparkCompiler "com.asakusafw.lang.compiler:asakusa-compiler-extension-directio:${COMPILER_PROJECT_VERSION}"
                asakusaSparkCompiler "com.asakusafw:asakusa-directio-vocabulary:${project.asakusafw.asakusafwVersion}"

                asakusaSparkCompiler "com.asakusafw.lang.compiler:asakusa-compiler-extension-windgate:${COMPILER_PROJECT_VERSION}"
                asakusaSparkCompiler "com.asakusafw:asakusa-windgate-vocabulary:${project.asakusafw.asakusafwVersion}"
            }
        }
    }

    private void defineTasks() {
        AsakusafwPluginConvention convention = project.asakusafw
        AsakusafwCompilerExtension spark = convention.spark
        project.task('sparkCompileBatchapps', type: AsakusaCompileTask) { AsakusaCompileTask task ->
            task.group AsakusafwPlugin.ASAKUSAFW_BUILD_GROUP
            task.description 'Compiles Asakusa DSL source files for Spark environment'
            task.dependsOn 'classes'
            project.tasks.assemble.dependsOn task

            task.toolClasspath << { project.configurations.asakusaSparkCompiler }
            task.toolClasspath << { project.sourceSets.main.compileClasspath }

            task.explore << { project.sourceSets.main.output.classesDir }
            task.attach << { project.configurations.embedded }

            task.customDataModelProcessors << { spark.customDataModelProcessors }
            task.customExternalPortProcessors << { spark.customExternalPortProcessors }
            task.customBatchProcessors << { spark.customBatchProcessors }
            task.customJobflowProcessors << { spark.customJobflowProcessors }
            task.customParticipants << { spark.customParticipants }

            task.conventionMapping.with {
                maxHeapSize = { convention.maxHeapSize }
                include = { spark.include }
                exclude = { spark.exclude }
                runtimeWorkingDirectory = { spark.runtimeWorkingDirectory }
                batchIdPrefix = { spark.batchIdPrefix }
                outputDirectory = { project.file(spark.outputDirectory) }
                failOnError = { spark.failOnError }
            }
        }
        project.afterEvaluate {
            AsakusaCompileTask task = project.tasks.sparkCompileBatchapps
            Map<String, String> map = [:]
            map.putAll(ResolutionUtils.resolveToStringMap(spark.compilerProperties))
            map.putAll(ResolutionUtils.resolveToStringMap(task.compilerProperties))
            task.compilerProperties = map

            if (convention.logbackConf != null) {
                File f = project.file(convention.logbackConf)
                task.systemProperties.put('logback.configurationFile', f.absolutePath)
            }
        }
    }
}
