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

import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.FileCopyDetails

import com.asakusafw.gradle.plugins.AsakusafwOrganizerProfile
import com.asakusafw.gradle.plugins.internal.AbstractOrganizer
import com.asakusafw.gradle.plugins.internal.PluginUtils
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
                SparkDist : [
                    "com.asakusafw.spark:asakusa-spark-assembly:${base.featureVersion}:dist@jar",
                    "com.asakusafw.spark:asakusa-spark-bootstrap:${base.featureVersion}:dist@jar",
                ],
                SparkLib : [
                    "com.asakusafw.bridge:asakusa-bridge-runtime-all:${base.langVersion}:lib@jar",
                    "com.asakusafw.spark:asakusa-spark-runtime:${base.featureVersion}@jar",
                    "com.asakusafw:asakusa-iterative-common:${base.coreVersion}@jar",
                    "com.asakusafw.spark.extensions:asakusa-spark-extensions-iterativebatch-runtime-core:${base.featureVersion}@jar",
                    "com.asakusafw.spark.extensions:asakusa-spark-extensions-iterativebatch-runtime-iterative:${base.featureVersion}@jar",
                    "com.asakusafw.spark:asakusa-spark-bootstrap:${base.featureVersion}:exec@jar",
                    "com.jsuereth:scala-arm_2.11:1.4@jar",
                ],
            ])
        }
    }

    private void configureTasks() {
        createAttachComponentTasks 'attachComponent', [
            Spark : {
                into('.') {
                    extract configuration('asakusafwSparkDist')
                    process {
                        filesMatching('**/spark/bin/spark-execute') { FileCopyDetails f ->
                            f.setMode(0755)
                        }
                    }
                }
                into('spark/lib') {
                    put configuration('asakusafwSparkLib')
                    process {
                        rename(/(asakusa-spark-bootstrap)-.*-exec\.jar/, '$1.jar')
                    }
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
                project.logger.info "Enabling Asakusa on Spark: ${profile.name}"
                task('attachAssemble').dependsOn task('attachComponentSpark')
                PluginUtils.afterTaskEnabled(project, AsakusaSparkSdkPlugin.TASK_COMPILE) { Task compiler ->
                    task('attachSparkBatchapps').dependsOn compiler
                    if (profile.batchapps.isEnabled()) {
                        project.logger.info "Enabling Spark Batchapps: ${profile.name}"
                        task('attachAssemble').dependsOn task('attachSparkBatchapps')
                    }
                }
            }
        }
    }
}
