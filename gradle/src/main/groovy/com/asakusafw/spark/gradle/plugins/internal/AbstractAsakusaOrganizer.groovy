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

import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.util.ConfigureUtil

import com.asakusafw.gradle.plugins.AsakusafwOrganizerProfile
import com.asakusafw.gradle.plugins.internal.AsakusafwInternalPluginConvention.DependencyConfiguration
import com.asakusafw.spark.gradle.plugins.AsakusafwOrganizerSparkExtension

/**
 * An abstract implementation of processing {@link AsakusafwOrganizerProfile}.
 * FIXME merge to main
 */
abstract class AbstractAsakusaOrganizer {

    /**
     * The current project.
     */
    final Project project

    /**
     * The target profile.
     */
    final AsakusafwOrganizerProfile profile

    /**
     * Creates a new instance.
     * @param project the current project
     * @param profile the target profile
     */
    AbstractAsakusaOrganizer(Project project, AsakusafwOrganizerProfile profile) {
        this.project = project
        this.profile = profile
    }

    /**
     * Configures the target profile.
     */
    abstract void configureProfile()

    /**
     * Returns the profile name.
     * @return the profile name
     */
    String getName() {
        return profile.name
    }

    protected void createConfigurations(String prefix, Map<String, String> confMap) {
        confMap.each { String name, String desc ->
            createConfiguration(prefix + name) {
                description desc
                visible false
            }
        }
    }

    protected void createDependencies(String prefix, Map<String, Object> depsMap) {
        depsMap.each { String name, Object value ->
            if (value instanceof Collection<?> || value instanceof Object[]) {
                value.each { Object notation ->
                    createDependency prefix + name, notation
                }
            } else if (value != null) {
                createDependency prefix + name, value
            }
        }
    }

    protected void createAttachComponentTasks(String prefix, Map<String, Closure<?>> actionMap) {
        actionMap.each { String name, Closure<?> action ->
            createTask(prefix + name) {
                doLast {
                    ConfigureUtil.configure action, profile.components
                }
            }
        }
    }

    protected boolean isProfileTask(Task task) {
        if (task.hasProperty('profileName')) {
            return task.profileName == profile.name
        }
        return false
    }

    protected String qualify(String name) {
        return "${name}_${profile.name}"
    }

    protected Configuration createConfiguration(String name, Closure<?> configurator) {
        return project.configurations.create(qualify(name), configurator)
    }

    protected Dependency createDependency(String configurationName, Object notation) {
        return project.dependencies.add(qualify(configurationName), notation)
    }

    protected Task createTask(String name, Class<? extends Task> parent, Closure<?> configurator) {
        project.tasks.create(qualify(name), parent, configurator)
        Task task = task(name)
        task.ext.profileName = profile.name
        return task
    }

    protected Task createTask(String name) {
        project.tasks.create(qualify(name))
        Task task = task(name)
        task.ext.profileName = profile.name
        return task
    }

    protected Task createTask(String name, Closure<?> configurator) {
        project.tasks.create(qualify(name), configurator)
        Task task = task(name)
        task.ext.profileName = profile.name
        return task
    }

    /**
     * Returns the task name for the profile.
     * @param name the bare task name
     * @return the corresponded profile task name
     */
    String taskName(String name) {
        return qualify(name)
    }

    /**
     * Returns the task for the profile.
     * @param name the bare task name
     * @return the corresponded profile task
     */
    Task task(String name) {
        return project.tasks[qualify(name)]
    }

    /**
     * Returns the configuration for the profile.
     * @param name the bare task name
     * @return the corresponded profile configuration
     */
    Configuration configuration(String name) {
        return project.configurations[qualify(name)]
    }
}
