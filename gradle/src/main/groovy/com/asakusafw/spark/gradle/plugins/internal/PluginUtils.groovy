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
import org.gradle.api.ProjectState;
import org.gradle.util.GradleVersion

/**
 * Basic utilities for Gradle plug-ins.
 * FIXME merge to main
 */
final class PluginUtils {

    /**
     * Executes a closure after the project was evaluated only if evaluation was not failed.
     * @param project the target project
     * @param closure the closure
     */
    static void afterEvaluate(Project project, Closure<?> closure) {
        project.afterEvaluate { Project p, ProjectState state ->
            if (state.failure != null) {
                return
            }
            closure.call(project)
        }
    }

    /**
     * Compares the target Gradle version with the current Gradle version.
     * @param version the target Gradle version
     * @return {@code =0} - same, {@code >0} - the current version is newer, {@code <0} - the current version is older
     */
    static int compareGradleVersion(String version) {
        GradleVersion current = GradleVersion.current()
        GradleVersion target = GradleVersion.version(version)
        return current.compareTo(target)
    }

    /**
     * Calls a closure after the target plug-in is enabled.
     * @param project the current project
     * @param pluginType the target plug-in class
     * @param closure the closure
     */
    static void afterPluginEnabled(Project project, Class<?> pluginType, Closure<?> closure) {
        project.plugins.withType(pluginType) {
            closure.call()
        }
    }

    /**
     * Calls a closure after the target plug-in is enabled.
     * @param project the current project
     * @param pluginType the target plug-in class
     * @param closure the closure
     */
    static void afterPluginEnabled(Project project, String pluginId, Closure<?> closure) {
        if (compareGradleVersion('2.0') >= 0) {
            project.plugins.withId(pluginId) {
                closure.call()
            }
        } else {
            project.plugins.matching({ it == project.plugins.findPlugin(pluginId) }).all {
                closure.call()
            }
        }
    }

    private PluginUtils() {
    }
}
