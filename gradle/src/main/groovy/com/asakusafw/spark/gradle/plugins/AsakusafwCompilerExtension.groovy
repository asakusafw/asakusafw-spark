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
package com.asakusafw.spark.gradle.plugins

/**
 * An extension object for Asakusa DSL compilers.
 */
class AsakusafwCompilerExtension {

    /**
     * The base output directory for compilation results.
     * This path will be resolved using {@code project.file(...)}.
     */
    Object outputDirectory

    /**
     * The accepting batch class name pattern ({@code "*"} as a wildcard character).
     */
    String include

    /**
     * The ignoring batch class name pattern ({@code "*"} as a wildcard character).
     */
    String exclude

    /**
     * The custom runtime working directory URI.
     */
    String runtimeWorkingDirectory

    /**
     * The compiler properties.
     */
    Map<Object, Object> compilerProperties = [:]

    /**
     * The custom batch ID prefix for applications.
     */
    String batchIdPrefix

    /**
     * Whether fails on compilation errors or not.
     */
    boolean failOnError
}
