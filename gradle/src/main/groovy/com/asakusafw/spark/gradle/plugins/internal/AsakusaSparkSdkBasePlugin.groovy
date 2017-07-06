/*
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
package com.asakusafw.spark.gradle.plugins.internal

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.DependencyResolveDetails
import org.gradle.api.artifacts.ModuleVersionSelector
import org.gradle.api.artifacts.ResolutionStrategy

import com.asakusafw.gradle.plugins.AsakusafwSdkExtension
import com.asakusafw.gradle.plugins.internal.AsakusaSdkPlugin
import com.asakusafw.gradle.plugins.internal.PluginUtils
import com.asakusafw.lang.gradle.plugins.internal.AsakusaLangSdkPlugin

/**
 * A base plug-in of {@link AsakusaSparkSdkPlugin}.
 * This only organizes dependencies and testkits.
 * @since 0.4.0
 */
class AsakusaSparkSdkBasePlugin implements Plugin<Project> {

    private Project project

    @Override
    void apply(Project project) {
        this.project = project

        project.apply plugin: AsakusaLangSdkPlugin
        project.apply plugin: AsakusaSparkBasePlugin

        configureTestkit()
        configureConfigurations()
    }

    private void configureTestkit() {
        AsakusafwSdkExtension sdk = AsakusaSdkPlugin.get(project).sdk
        sdk.availableTestkits << new AsakusaSparkTestkit()
    }

    private void configureConfigurations() {
        project.configurations {
            asakusaSparkCommon {
                description 'Common libraries of Asakusa DSL Compiler for Spark'
                extendsFrom project.configurations.asakusaLangCommon
                exclude group: 'asm', module: 'asm'
            }
            asakusaSparkCompiler {
                description 'Full classpath of Asakusa DSL Compiler for Spark'
                extendsFrom project.configurations.asakusaLangCompiler
                extendsFrom project.configurations.asakusaSparkCommon
            }
            asakusaSparkTestkit {
                description 'Asakusa DSL testkit classpath for Spark'
                extendsFrom project.configurations.asakusaLangTestkit
                extendsFrom project.configurations.asakusaSparkCommon
            }
        }
        PluginUtils.afterEvaluate(project) {
            AsakusaSparkBaseExtension base = AsakusaSparkBasePlugin.get(project)
            AsakusafwSdkExtension features = AsakusaSdkPlugin.get(project).sdk
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
                if (features.core) {
                    asakusaSparkCommon("com.asakusafw.spark:asakusa-spark-compiler:${base.featureVersion}") {
                        exclude module: 'hadoop-client'
                    }
                    asakusaSparkCommon "com.asakusafw.iterative:asakusa-compiler-extension-iterative:${base.langVersion}"
                    asakusaSparkCommon "com.asakusafw.spark.extensions:asakusa-spark-extensions-iterativebatch-compiler-iterative:${base.featureVersion}"
                }
                if (features.testing) {
                    asakusaSparkTestkit "com.asakusafw.spark:asakusa-spark-test-adapter:${base.featureVersion}"
                    asakusaSparkTestkit "com.asakusafw.bridge:asakusa-bridge-runtime-all:${base.langVersion}"
                    asakusaSparkTestkit "com.asakusafw.spark:asakusa-spark-runtime:${base.featureVersion}"
                    asakusaSparkTestkit "com.asakusafw.spark.extensions:asakusa-spark-extensions-iterativebatch-runtime-core:${base.featureVersion}"
                    asakusaSparkTestkit "com.asakusafw.spark.extensions:asakusa-spark-extensions-iterativebatch-runtime-iterative:${base.featureVersion}"
                }
            }
        }
    }
}
