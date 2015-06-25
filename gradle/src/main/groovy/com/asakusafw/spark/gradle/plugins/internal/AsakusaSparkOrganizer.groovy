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

import org.gradle.api.Project

import com.asakusafw.gradle.plugins.AsakusafwOrganizerProfile
import com.asakusafw.spark.gradle.plugins.AsakusafwOrganizerSparkExtension

/**
 * Processes an {@link AsakusafwOrganizerProfile} for Spark environment.
 */
class AsakusaSparkOrganizer extends AbstractOrganizer {

    /**
     * Creates a new instance.
     * @param project the current project
     * @param profile the target profile
     */
    AsakusaSparkOrganizer(Project project, AsakusafwOrganizerProfile profile) {
        super(project, profile)
    }

    /**
     * Configures the target profile.
     */
    @Override
    void configureProfile() {
        configureConfigurations()
        configureDependencies()
        configureTasks()
        enableTasks()
    }

    private void configureConfigurations() {
        createConfigurations('asakusafw', [
            SparkDist : "Contents of Asakusa on Spark modules (${profile.name}).",
            SparkLib : "Libraries of Asakusa on Spark modules (${profile.name}).",
        ])
    }

    private void configureDependencies() {
        PluginUtils.afterEvaluate(project) {
            AsakusaSparkBaseExtension base = AsakusaSparkBasePlugin.get(project)
            createDependencies('asakusafw', [
                SparkDist : "com.asakusafw.spark:asakusa-spark-assembly:${base.sparkProjectVersion}:dist@jar",
                SparkLib : [
                    "com.asakusafw.bridge:asakusa-bridge-runtime-all:${base.compilerProjectVersion}:lib@jar",
                    "com.asakusafw.spark:asakusa-spark-runtime:${base.sparkProjectVersion}@jar",
                ],
            ])
        }
    }

    private void configureTasks() {
        createAttachComponentTasks 'attachComponent', [
            Spark : {
                into('.') {
                    extract configuration('asakusafwSparkDist')
                }
                into('spark/lib') {
                    put configuration('asakusafwSparkLib')
                }
            },
        ]
        createAttachComponentTasks 'attach', [
            SparkBatchapps : {
                into('batchapps') {
                    put project.asakusafw.spark.outputDirectory
                }
            },
        ]
    }

    private void enableTasks() {
        PluginUtils.afterEvaluate(project) {
            AsakusafwOrganizerSparkExtension spark = profile.spark
            if (spark.isEnabled()) {
                project.logger.info 'Enabling Asakusa on Spark'
                task('attachAssemble').dependsOn task('attachComponentSpark')
            }
            if (profile.batchapps.isEnabled() && project.plugins.hasPlugin('asakusafw-spark')) {
                project.logger.info 'Enabling Spark Batchapps'
                task('attachSparkBatchapps').shouldRunAfter project.tasks.sparkCompileBatchapps
                task('attachAssemble').dependsOn task('attachSparkBatchapps')
            }
        }
    }
}
