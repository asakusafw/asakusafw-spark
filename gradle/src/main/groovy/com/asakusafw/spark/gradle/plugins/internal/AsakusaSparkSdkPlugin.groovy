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

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.DependencyResolveDetails
import org.gradle.api.artifacts.ModuleVersionSelector
import org.gradle.api.artifacts.ResolutionStrategy
import org.gradle.api.artifacts.ResolvedConfiguration
import org.gradle.api.artifacts.ResolvedDependency

import com.asakusafw.gradle.plugins.AsakusafwBasePlugin
import com.asakusafw.gradle.plugins.AsakusafwCompilerExtension
import com.asakusafw.gradle.plugins.AsakusafwPluginConvention
import com.asakusafw.gradle.plugins.internal.AsakusaSdkPlugin
import com.asakusafw.gradle.plugins.internal.PluginUtils
import com.asakusafw.gradle.tasks.AsakusaCompileTask
import com.asakusafw.gradle.tasks.internal.ResolutionUtils

/**
 * A Gradle sub plug-in for Asakusa on Spark SDK.
 */
class AsakusaSparkSdkPlugin implements Plugin<Project> {

    public static final String TASK_COMPILE = 'sparkCompileBatchapps'

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

        project.apply plugin: 'asakusafw-sdk'
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
            asakusaSparkCommon {
                description 'Common libraries of Asakusa DSL Compiler for Spark'
                exclude group: 'asm', module: 'asm'
            }
            asakusaSparkCompiler {
                description 'Full classpath of Asakusa DSL Compiler for Spark'
                extendsFrom project.configurations.compile
                extendsFrom project.configurations.asakusaSparkCommon
            }
            asakusaSparkTestkit {
                description 'Asakusa DSL testkit classpath for Spark'
                extendsFrom project.configurations.asakusaSparkCommon
                exclude group: 'com.asakusafw', module: 'asakusa-test-mapreduce'
            }
        }
        PluginUtils.afterEvaluate(project) {
            AsakusaSparkBaseExtension base = AsakusaSparkBasePlugin.get(project)
            AsakusafwPluginConvention asakusa = project.asakusafw
            project.configurations {
                asakusaSparkCommon { Configuration conf ->
                    if (base.customSparkArtifact != null) {
                        conf.resolutionStrategy { ResolutionStrategy strategy ->
                            strategy.eachDependency { DependencyResolveDetails details ->
                                ModuleVersionSelector req = details.requested
                                if (req.group == 'org.apache.spark' && req.name.startsWith('spark-core_')) {
                                    details.useTarget base.customSparkArtifact
                                }
                            }
                        }
                    }
                }
                asakusaSparkCompiler { Configuration conf ->
                    base.excludeModules.each { Object moduleInfo ->
                        project.logger.info "excludes module for Spark compiler: ${moduleInfo}"
                        if (moduleInfo instanceof Map<?, ?>) {
                            conf.exclude moduleInfo
                        } else {
                            conf.exclude module: moduleInfo
                        }
                    }
                }
            }
            project.dependencies {
                asakusaSparkCommon("com.asakusafw.spark:asakusa-spark-compiler:${base.sparkProjectVersion}") {
                    exclude module: 'hadoop-client'
                }

                asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-cli:${base.compilerProjectVersion}"
                asakusaSparkCommon "com.asakusafw:simple-graph:${asakusa.asakusafwVersion}"
                asakusaSparkCommon "com.asakusafw:java-dom:${asakusa.asakusafwVersion}"

                asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-cleanup:${base.compilerProjectVersion}"
                asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-redirector:${base.compilerProjectVersion}"
                asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-yaess:${base.compilerProjectVersion}"
                asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-directio:${base.compilerProjectVersion}"
                asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-windgate:${base.compilerProjectVersion}"
                asakusaSparkCommon "com.asakusafw.iterative:asakusa-compiler-extension-iterative:${base.compilerProjectVersion}"
                asakusaSparkCommon "com.asakusafw.spark.extensions:asakusa-spark-extensions-iterativebatch-compiler-iterative:${base.sparkProjectVersion}"

                asakusaSparkCompiler "com.asakusafw:asakusa-dsl-vocabulary:${asakusa.asakusafwVersion}"
                asakusaSparkCompiler "com.asakusafw:asakusa-runtime:${asakusa.asakusafwVersion}"
                asakusaSparkCompiler "com.asakusafw:asakusa-yaess-core:${asakusa.asakusafwVersion}"
                asakusaSparkCompiler "com.asakusafw:asakusa-directio-vocabulary:${asakusa.asakusafwVersion}"
                asakusaSparkCompiler "com.asakusafw:asakusa-windgate-vocabulary:${asakusa.asakusafwVersion}"

                asakusaSparkTestkit "com.asakusafw.spark:asakusa-spark-test-adapter:${base.sparkProjectVersion}"
                asakusaSparkTestkit "com.asakusafw.bridge:asakusa-bridge-runtime-all:${base.compilerProjectVersion}"
                asakusaSparkTestkit "com.asakusafw.spark:asakusa-spark-runtime:${base.sparkProjectVersion}"
                asakusaSparkTestkit "com.asakusafw.spark.extensions:asakusa-spark-extensions-iterativebatch-runtime-core:${base.sparkProjectVersion}"
                asakusaSparkTestkit "com.asakusafw.spark.extensions:asakusa-spark-extensions-iterativebatch-runtime-iterative:${base.sparkProjectVersion}"
            }
        }
    }

    private void defineTasks() {
        AsakusafwPluginConvention convention = project.asakusafw
        AsakusafwCompilerExtension spark = convention.spark
        project.tasks.create(TASK_COMPILE, AsakusaCompileTask) { AsakusaCompileTask task ->
            task.group AsakusaSdkPlugin.ASAKUSAFW_BUILD_GROUP
            task.description 'Compiles Asakusa DSL source files for Spark environment'
            task.dependsOn 'classes'

            task.compilerName = 'Asakusa DSL compiler for Spark'

            task.launcherClasspath << { project.configurations.asakusaToolLauncher }

            task.toolClasspath << { project.configurations.asakusaSparkCompiler }
            task.toolClasspath << { project.sourceSets.main.compileClasspath - project.configurations.compile }

            task.explore << { [project.sourceSets.main.output.classesDir].findAll { it.exists() } }
            task.embed << { [project.sourceSets.main.output.resourcesDir].findAll { it.exists() } }
            task.attach << { project.configurations.embedded }

            task.include << { spark.include }
            task.exclude << { spark.exclude }

            task.clean = true

            task.conventionMapping.with {
                maxHeapSize = { convention.maxHeapSize }
                runtimeWorkingDirectory = { spark.runtimeWorkingDirectory }
                batchIdPrefix = { spark.batchIdPrefix }
                outputDirectory = { project.file(spark.outputDirectory) }
                failOnError = { spark.failOnError }
            }
            project.tasks.compileBatchapp.dependsOn task
            project.tasks.jarBatchapp.from { task.outputDirectory }
        }
        extendVersionsTask()
        PluginUtils.afterEvaluate(project) {
            AsakusaCompileTask task = project.tasks.getByName(TASK_COMPILE)
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

    private void extendVersionsTask() {
        project.tasks.getByName(AsakusafwBasePlugin.TASK_VERSIONS) << {
            def sparkVersion = 'INVALID'
            try {
                logger.info 'detecting Spark version'
                ResolvedConfiguration conf = project.configurations.asakusaSparkCompiler.resolvedConfiguration
                sparkVersion = findSparkVersion(conf) ?: 'UNKNOWN'
            } catch (Exception e) {
                logger.info 'failed to detect Spark version', e
            }
            logger.lifecycle "Spark: ${sparkVersion}"
        }
    }

    private String findSparkVersion(ResolvedConfiguration conf) {
        LinkedList<ResolvedDependency> work = new LinkedList<ResolvedDependency>()
        work.addAll(conf.firstLevelModuleDependencies)
        Set<ResolvedDependency> saw = new HashSet<ResolvedDependency>()
        while (!work.empty) {
            ResolvedDependency d = work.removeFirst()
            if (saw.contains(d)) {
                continue
            }
            saw.add(d)
            if (d.moduleVersion != null
                    && d.moduleGroup == 'org.apache.spark'
                    && d.moduleName != null
                    && d.moduleName.startsWith('spark-core_')) {
                return d.moduleVersion
            }
            work.addAll(d.children)
        }
        return null
    }
}
