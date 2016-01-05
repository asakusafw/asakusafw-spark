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

/**
 * A Gradle sub plug-in for Asakusa on Spark compiler.
 */
class AsakusaSparkBasePlugin implements Plugin<Project> {

    private static final String ARTIFACT_INFO_PATH = 'META-INF/asakusa-spark-gradle/artifact.properties'

    private static final String INVALID_VERSION = 'INVALID'

    private static final List<Object> EXCLUDE_MODULES = [

        [group: 'asm', module: 'asm'],

        [group: 'org.slf4j', module: 'slf4j-jdk14'],
        [group: 'org.slf4j', module: 'slf4j-jcl'],
        [group: 'org.slf4j', module: 'slf4j-log4j12'],

        'junit',
        'mockito-all',

        'ashigel-compiler',
        'asakusa-directio-plugin',
        'asakusa-windgate-plugin',
        'asakusa-thundergate-plugin',
        'asakusa-yaess-plugin',
        'asakusa-dsl-analysis-plugin',

        'asakusa-test-driver',
        'asakusa-test-data-generator',
        'asakusa-test-data-provider',
        'asakusa-test-moderator',
        'asakusa-directio-test-moderator',
        'asakusa-windgate-test-moderator',
        'asakusa-thundergate-test-moderator',
    ]

    private Project project

    private AsakusaSparkBaseExtension extension

    /**
     * Applies this plug-in and returns the extension object for the project.
     * @param project the target project
     * @return the corresponded extension
     */
    static AsakusaSparkBaseExtension get(Project project) {
        project.apply plugin: AsakusaSparkBasePlugin
        return project.plugins.getPlugin(AsakusaSparkBasePlugin).extension
    }

    @Override
    void apply(Project project) {
        this.project = project
        this.extension = project.extensions.create('asakusaSparkBase', AsakusaSparkBaseExtension)
        configureExtension()
    }

    private void configureExtension() {
        configureArtifactVersion()
        extension.excludeModules.addAll(EXCLUDE_MODULES)
    }

    private void configureArtifactVersion() {
        InputStream input = getClass().classLoader.getResourceAsStream(ARTIFACT_INFO_PATH)
        if (input != null) {
            try {
                Properties properties = new Properties()
                properties.load(input)
                extension.compilerProjectVersion = properties.getProperty('compiler-version', INVALID_VERSION)
                extension.sparkProjectVersion = properties.getProperty('spark-compiler-version', INVALID_VERSION)
            } catch (IOException e) {
                project.logger.warn "error occurred while extracting artifact version: ${ARTIFACT_INFO_PATH}"
            } finally {
                input.close()
            }
        }
        if (extension.compilerProjectVersion == null) {
            project.logger.warn "failed to detect version of Asakusa DSL Compiler Core: ${ARTIFACT_INFO_PATH}"
            extension.compilerProjectVersion = null
        }
        project.logger.info "Asakusa DSL Compiler Core: ${extension.compilerProjectVersion}"
        if (extension.sparkProjectVersion == null) {
            project.logger.warn "failed to detect version of Asakusa DSL Compiler for Spark: ${ARTIFACT_INFO_PATH}"
            extension.sparkProjectVersion = null
        }
        project.logger.info "Asakusa DSL Compiler for Spark: ${extension.sparkProjectVersion}"
    }

    /**
     * Returns the extension.
     * @return the extension
     */
    AsakusaSparkBaseExtension getExtension() {
        return extension
    }
}
