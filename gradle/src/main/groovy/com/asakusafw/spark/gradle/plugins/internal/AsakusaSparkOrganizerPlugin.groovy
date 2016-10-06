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

import org.gradle.api.NamedDomainObjectCollection
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task

import com.asakusafw.gradle.plugins.AsakusafwOrganizerPlugin
import com.asakusafw.gradle.plugins.AsakusafwOrganizerPluginConvention
import com.asakusafw.gradle.plugins.AsakusafwOrganizerProfile
import com.asakusafw.gradle.plugins.internal.PluginUtils
import com.asakusafw.spark.gradle.plugins.AsakusafwOrganizerSparkExtension

/**
 * A Gradle sub plug-in for Asakusa on Spark project organizer.
 */
class AsakusaSparkOrganizerPlugin implements Plugin<Project> {

    private Project project

    private NamedDomainObjectCollection<AsakusaSparkOrganizer> organizers

    @Override
    void apply(Project project) {
        this.project = project
        this.organizers = project.container(AsakusaSparkOrganizer)

        project.apply plugin: 'asakusafw-organizer'
        project.apply plugin: AsakusaSparkBasePlugin

        configureConvention()
        configureProfiles()
        configureTasks()
    }

    /**
     * Returns the organizers for each profile (only for testing).
     * @return the organizers for each profile
     */
    NamedDomainObjectCollection<AsakusaSparkOrganizer> getOrganizers() {
        return organizers
    }

    private void configureConvention() {
        AsakusaSparkBaseExtension base = AsakusaSparkBasePlugin.get(project)
        AsakusafwOrganizerPluginConvention convention = project.asakusafwOrganizer
        convention.extensions.create('spark', AsakusafwOrganizerSparkExtension)
        convention.spark.conventionMapping.with {
            enabled = { true }
        }
        convention.yaess.conventionMapping.with {
            iterativeEnabled = { true }
        }
        PluginUtils.injectVersionProperty(convention.spark, { base.featureVersion })
    }

    private void configureProfiles() {
        AsakusafwOrganizerPluginConvention convention = project.asakusafwOrganizer
        convention.profiles.all { AsakusafwOrganizerProfile profile ->
            configureProfile(profile)
        }
    }

    private void configureProfile(AsakusafwOrganizerProfile profile) {
        AsakusaSparkBaseExtension base = AsakusaSparkBasePlugin.get(project)
        profile.extensions.create('spark', AsakusafwOrganizerSparkExtension)
        profile.spark.conventionMapping.with {
            enabled = { project.asakusafwOrganizer.spark.enabled }
        }
        PluginUtils.injectVersionProperty(profile.spark, { base.featureVersion })

        AsakusaSparkOrganizer organizer = new AsakusaSparkOrganizer(project, profile)
        organizer.configureProfile()
        organizers << organizer
    }

    private void configureTasks() {
        defineFacadeTasks([
            attachComponentSpark : 'Attaches Asakusa on Spark components to assemblies.',
            attachSparkBatchapps : 'Attaches Asakusa on Spark batch applications to assemblies.',
        ])
    }

    private void defineFacadeTasks(Map<String, String> taskMap) {
        taskMap.each { String taskName, String desc ->
            project.task(taskName) { Task task ->
                if (desc != null) {
                    task.group AsakusafwOrganizerPlugin.ASAKUSAFW_ORGANIZER_GROUP
                    task.description desc
                }
                organizers.all { AsakusaSparkOrganizer organizer ->
                    task.dependsOn organizer.task(task.name)
                }
            }
        }
    }
}
