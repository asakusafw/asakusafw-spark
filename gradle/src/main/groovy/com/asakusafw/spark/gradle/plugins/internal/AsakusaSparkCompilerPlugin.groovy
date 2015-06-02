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

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration

import com.asakusafw.gradle.plugins.AsakusafwCompilerExtension
import com.asakusafw.gradle.plugins.AsakusafwPlugin
import com.asakusafw.gradle.plugins.AsakusafwPluginConvention
import com.asakusafw.gradle.tasks.AsakusaCompileTask
import com.asakusafw.gradle.tasks.internal.ResolutionUtils

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

    private AsakusaSparkBaseExtension base

    @Override
    void apply(Project project) {
        this.project = project

        project.apply plugin: 'asakusafw'
        project.apply plugin: AsakusaSparkBasePlugin

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
            AsakusaSparkBaseExtension base = AsakusaSparkBasePlugin.get(project)
            AsakusafwPluginConvention asakusa = project.asakusafw
            project.configurations {
                asakusaSparkCompiler { Configuration conf ->
                    base.excludeModules.each { String moduleName ->
                        project.logger.info "excludes module for Spark compiler: ${moduleName}"
                        conf.exclude module: moduleName
                    }
                }
            }
            project.dependencies {
                asakusaSparkCompiler "com.asakusafw.spark:asakusa-spark-compiler-core:${base.sparkProjectVersion}"
                asakusaSparkCompiler(base.sparkArtifact) {
                    exclude module: 'hadoop-client'
                }

                asakusaSparkCompiler "com.asakusafw.lang.compiler:asakusa-compiler-cli:${base.compilerProjectVersion}"
                asakusaSparkCompiler "com.asakusafw:asakusa-dsl-vocabulary:${asakusa.asakusafwVersion}"
                asakusaSparkCompiler "com.asakusafw:simple-graph:${asakusa.asakusafwVersion}"
                asakusaSparkCompiler "com.asakusafw:java-dom:${asakusa.asakusafwVersion}"

                asakusaSparkCompiler "com.asakusafw.lang.compiler:asakusa-compiler-extension-cleanup:${base.compilerProjectVersion}"
                asakusaSparkCompiler "com.asakusafw.lang.compiler:asakusa-compiler-extension-redirector:${base.compilerProjectVersion}"

                asakusaSparkCompiler "com.asakusafw.lang.compiler:asakusa-compiler-extension-yaess:${base.compilerProjectVersion}"
                asakusaSparkCompiler "com.asakusafw:asakusa-yaess-core:${asakusa.asakusafwVersion}"

                asakusaSparkCompiler "com.asakusafw.lang.compiler:asakusa-compiler-extension-directio:${base.compilerProjectVersion}"
                asakusaSparkCompiler "com.asakusafw:asakusa-directio-vocabulary:${asakusa.asakusafwVersion}"

                asakusaSparkCompiler "com.asakusafw.lang.compiler:asakusa-compiler-extension-windgate:${base.compilerProjectVersion}"
                asakusaSparkCompiler "com.asakusafw:asakusa-windgate-vocabulary:${asakusa.asakusafwVersion}"
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
            task.toolClasspath << { project.sourceSets.main.compileClasspath - project.configurations.compile }

            task.explore << { project.sourceSets.main.output.classesDir }
            task.attach << { project.configurations.embedded }

            task.include << { spark.include }
            task.exclude << { spark.exclude }

            task.conventionMapping.with {
                maxHeapSize = { convention.maxHeapSize }
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
