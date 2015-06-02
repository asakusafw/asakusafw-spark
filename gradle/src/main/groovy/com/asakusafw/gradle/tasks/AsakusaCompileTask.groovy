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
package com.asakusafw.gradle.tasks

import groovy.transform.PackageScope

import org.gradle.api.DefaultTask
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.SkipWhenEmpty
import org.gradle.api.tasks.TaskAction
import org.gradle.process.JavaExecSpec

import com.asakusafw.gradle.tasks.internal.ResolutionUtils

/**
 * Gradle Task for Asakusa DSL Compiler.
 */
class AsakusaCompileTask extends DefaultTask {

    /**
     * The maximum heap size.
     */
    String maxHeapSize

    /**
     * The compiler class libraries.
     */
    List<Object> toolClasspath = []

    @InputFiles
    FileCollection getToolClasspathFiles() {
        return collectFiles(getToolClasspath())
    }

    /**
     * The Java system properties.
     */
    Map<Object, Object> systemProperties = [:]

    @Input
    Map<String, String> getResolvedSystemProperties() {
        return ResolutionUtils.resolveToStringMap(getSystemProperties())
    }

    /**
     * The Java VM arguments.
     */
    List<Object> jvmArgs = []

    @Input
    List<String> getResolvedJvmArgs() {
        return ResolutionUtils.resolveToStringList(getJvmArgs())
    }

    /**
     * The library paths with batch classes.
     */
    List<Object> explore = []

    @SkipWhenEmpty
    @InputFiles
    FileCollection getExploreFiles() {
        return collectFiles(getExplore())
    }

    /**
     * The library paths to be attached to each batch package.
     */
    List<Object> attach = []

    @InputFiles
    FileCollection getAttachFiles() {
        return collectFiles(getAttach())
    }

    /**
     * The library paths to be embedded to each jobflow package.
     */
    List<Object> embed = []

    @InputFiles
    FileCollection getEmbedFiles() {
        return collectFiles(getEmbed())
    }

    /**
     * The external library paths.
     */
    List<Object> external = []

    @InputFiles
    FileCollection getExternalFiles() {
        return collectFiles(getExternal())
    }

    /**
     * The accepting batch class name pattern ({@code "*"} as a wildcard character).
     */
    @Optional
    @Input
    String include

    /**
     * The ignoring batch class name pattern ({@code "*"} as a wildcard character).
     */
    @Optional
    @Input
    String exclude

    /**
     * The custom data model processor classes.
     */
    List<Object> customDataModelProcessors = []

    @Input
    List<String> getResolvedCustomDataModelProcessors() {
        return ResolutionUtils.resolveToStringList(getCustomDataModelProcessors())
    }

    /**
     * The custom external port processor classes.
     */
    List<Object> customExternalPortProcessors = []

    @Input
    List<String> getResolvedCustomExternalPortProcessors() {
        return ResolutionUtils.resolveToStringList(getCustomExternalPortProcessors())
    }

    /**
     * The custom jobflow processor classes.
     */
    List<Object> customJobflowProcessors = []

    @Input
    List<String> getResolvedCustomJobflowProcessors() {
        return ResolutionUtils.resolveToStringList(getCustomJobflowProcessors())
    }

    /**
     * The custom batch processor classes.
     */
    List<Object> customBatchProcessors = []

    @Input
    List<String> getResolvedCustomBatchProcessors() {
        return ResolutionUtils.resolveToStringList(getCustomBatchProcessors())
    }

    /**
     * The custom compiler participant classes.
     */
    List<Object> customParticipants = []

    @Input
    List<String> getResolvedCustomParticipants() {
        return ResolutionUtils.resolveToStringList(getCustomParticipants())
    }

    /**
     * The custom runtime working directory URI.
     */
    @Optional
    @Input
    String runtimeWorkingDirectory

    /**
     * The compiler properties.
     */
    Map<Object, Object> compilerProperties = [:]

    @Input
    Map<String, String> getResolvedCompilerProperties() {
        return ResolutionUtils.resolveToStringMap(getCompilerProperties())
    }

    /**
     * The batch ID prefix for 'on-Spark' applications.
     */
    @Optional
    @Input
    String batchIdPrefix

    /**
     * The batch application output base path.
     */
    @OutputDirectory
    File outputDirectory

    /**
     * Whether fails on compilation errors or not.
     */
    boolean failOnError

    private FileCollection collectFiles(Object files) {
        Object all = project.files(files).collect { File f ->
            if (f.isFile()) {
                return [f]
            } else if (f.isDirectory()) {
                return project.fileTree(f)
            }
        }

        return project.files(all)
    }

    /**
     * Task Action of this task.
     */
    @TaskAction
    void perform() {
        String timestamp = new Date().format("yyyy-MM-dd HH:mm:ss (z)")
        project.delete(getOutputDirectory())
        project.mkdir(getOutputDirectory())

        project.javaexec { JavaExecSpec spec ->
            spec.main = 'com.asakusafw.lang.compiler.cli.BatchCompilerCli'
            spec.classpath = project.files(getToolClasspath())
            spec.jvmArgs = getResolvedJvmArgs()
            if (getMaxHeapSize() != null) {
                spec.maxHeapSize = getMaxHeapSize()
            }
            spec.systemProperties += getResolvedSystemProperties()
            spec.systemProperties += [ 'com.asakusafw.batchapp.build.timestamp' : timestamp ]
            spec.systemProperties += [ 'com.asakusafw.batchapp.build.java.version' : System.properties['java.version'] ]
            spec.enableAssertions = true

            // project repository
            configureFiles(spec, '--explore', getExplore())
            configureFiles(spec, '--attach', getAttach())
            configureFiles(spec, '--embed', getEmbed())
            configureFiles(spec, '--external', getExternal())

            // batch class detection
            configureString(spec, '--include', getInclude())
            configureString(spec, '--exclude', getExclude())

            // custom compiler plug-ins
            configureClasses(spec, '--dataModelProcessors', getResolvedCustomDataModelProcessors())
            configureClasses(spec, '--externalPortProcessors', getResolvedCustomExternalPortProcessors())
            configureClasses(spec, '--jobflowProcessors', getResolvedCustomJobflowProcessors())
            configureClasses(spec, '--batchProcessors', getResolvedCustomBatchProcessors())
            configureClasses(spec, '--participants', getResolvedCustomParticipants())

            // other options
            configureString(spec, '--output', getOutputDirectory().getAbsolutePath())
            configureString(spec, '--runtimeWorkingDirectory', getRuntimeWorkingDirectory())
            configureString(spec, '--batchIdPrefix', getBatchIdPrefix())
            configureBoolean(spec, '--failOnError', getFailOnError())
            getResolvedCompilerProperties().each { k, v ->
                spec.args('--property', "${k}=${v}")
            }
        }
    }

    @PackageScope
    void configureBoolean(JavaExecSpec spec, String key, boolean value) {
        if (value == false) {
            return
        }
        logger.debug("Asakusa compiler option: ${key}")
        spec.args(key)
    }

    @PackageScope
    void configureString(JavaExecSpec spec, String key, Object value) {
        if (value == null) {
            return
        }
        String s = String.valueOf(value)
        if (s.isEmpty()) {
            return
        }
        logger.debug("Asakusa compiler option: ${key}=${s}")
        spec.args(key, s)
    }

    @PackageScope
    void configureFiles(JavaExecSpec spec, String key, Object files) {
        FileCollection f = project.files(files)
        if (f.isEmpty()) {
            return
        }
        configureString(spec, key, f.asPath)
    }

    @PackageScope
    void configureClasses(JavaExecSpec spec, String key, List<String> classes) {
        if (classes.isEmpty()) {
            return
        }
        configureString(spec, key, classes.join(','))
    }
}
