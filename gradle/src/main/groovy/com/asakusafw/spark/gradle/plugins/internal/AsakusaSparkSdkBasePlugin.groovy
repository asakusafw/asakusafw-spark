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

import com.asakusafw.gradle.plugins.AsakusafwCompilerExtension
import com.asakusafw.gradle.plugins.AsakusafwPluginConvention
import com.asakusafw.gradle.plugins.AsakusafwSdkExtension
import com.asakusafw.gradle.plugins.internal.AsakusaSdkPlugin
import com.asakusafw.gradle.plugins.internal.PluginUtils

/**
 * A base plug-in of {@link AsakusaSparkSdkPlugin}.
 * This only organizes conventions and dependencies.
 * @since 0.4.0
 */
class AsakusaSparkSdkBasePlugin implements Plugin<Project> {

    private static final Map<String, String> REDIRECT = [
            'com.asakusafw.runtime.core.BatchContext' : 'com.asakusafw.bridge.api.BatchContext',
            'com.asakusafw.runtime.core.Report' : 'com.asakusafw.bridge.api.Report',
            'com.asakusafw.runtime.directio.api.DirectIo' : 'com.asakusafw.bridge.directio.api.DirectIo',
    ]

    private Project project

    private AsakusafwCompilerExtension extension

    @Override
    void apply(Project project) {
        this.project = project

        project.apply plugin: AsakusaSdkPlugin
        project.apply plugin: AsakusaSparkBasePlugin
        this.extension = AsakusaSdkPlugin.get(project).extensions.create('spark', AsakusafwCompilerExtension)

        configureConvention()
        configureConfigurations()
    }

    private void configureConvention() {
        AsakusaSparkBaseExtension base = AsakusaSparkBasePlugin.get(project)
        AsakusafwPluginConvention sdk = AsakusaSdkPlugin.get(project)
        extension.conventionMapping.with {
            outputDirectory = { project.relativePath(new File(project.buildDir, 'spark-batchapps')) }
            batchIdPrefix = { (String) 'spark.' }
            failOnError = { true }
        }
        REDIRECT.each { k, v ->
            extension.compilerProperties.put((String) "redirector.rule.${k}", v)
        }
        extension.compilerProperties.put('javac.version', { sdk.javac.sourceCompatibility.toString() })
        PluginUtils.injectVersionProperty(extension, { base.featureVersion })
        sdk.sdk.availableTestkits << new AsakusaSparkTestkit()
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
            }
        }
        PluginUtils.afterEvaluate(project) {
            AsakusaSparkBaseExtension base = AsakusaSparkBasePlugin.get(project)
            AsakusafwPluginConvention asakusa = project.asakusafw
            AsakusafwSdkExtension features = asakusa.sdk
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

                    asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-cli:${base.langVersion}"
                    asakusaSparkCommon "com.asakusafw:simple-graph:${asakusa.asakusafwVersion}"
                    asakusaSparkCommon "com.asakusafw:java-dom:${asakusa.asakusafwVersion}"

                    asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-cleanup:${base.langVersion}"
                    asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-redirector:${base.langVersion}"
                    asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-yaess:${base.langVersion}"

                    asakusaSparkCompiler "com.asakusafw:asakusa-dsl-vocabulary:${asakusa.asakusafwVersion}"
                    asakusaSparkCompiler "com.asakusafw:asakusa-runtime:${asakusa.asakusafwVersion}"
                    asakusaSparkCompiler "com.asakusafw:asakusa-yaess-core:${asakusa.asakusafwVersion}"

                    asakusaSparkCommon "com.asakusafw.iterative:asakusa-compiler-extension-iterative:${base.langVersion}"
                    asakusaSparkCommon "com.asakusafw.spark.extensions:asakusa-spark-extensions-iterativebatch-compiler-iterative:${base.featureVersion}"

                    if (features.directio) {
                        asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-directio:${base.langVersion}"
                        asakusaSparkCompiler "com.asakusafw:asakusa-directio-vocabulary:${asakusa.asakusafwVersion}"
                    }
                    if (features.windgate) {
                        asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-windgate:${base.langVersion}"
                        asakusaSparkCompiler "com.asakusafw:asakusa-windgate-vocabulary:${asakusa.asakusafwVersion}"
                    }
                    if (features.hive) {
                        asakusaSparkCommon "com.asakusafw.lang.compiler:asakusa-compiler-extension-hive:${base.langVersion}"
                    }
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

    /**
     * Returns the extension object of this plug-in.
     * The plug-in will be applied automatically.
     * @param project the target project
     * @return the related extension
     */
    static AsakusafwCompilerExtension get(Project project) {
        project.apply plugin: AsakusaSparkSdkBasePlugin
        AsakusaSparkSdkBasePlugin plugin = project.plugins.getPlugin(AsakusaSparkSdkBasePlugin)
        if (plugin == null) {
            throw new IllegalStateException('AsakusaSparkSdkBasePlugin has not been applied')
        }
        return plugin.extension
    }
}
