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
     * The accepting batch class name patterns ({@code "*"} as a wildcard character).
     */
    List<Object> include = []

    @Input
    List<String> getResolvedInclude() {
        return ResolutionUtils.resolveToStringList(getInclude())
    }

    /**
     * The ignoring batch class name patterns ({@code "*"} as a wildcard character).
     */
    List<Object> exclude = []

    @Input
    List<String> getResolvedExclude() {
        return ResolutionUtils.resolveToStringList(getExclude())
    }

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
        project.delete(getOutputDirectory())
        project.mkdir(getOutputDirectory())
        project.javaexec { JavaExecSpec spec ->
            spec.main = 'com.asakusafw.lang.compiler.cli.BatchCompilerCli'
            spec.classpath = project.files(getToolClasspath())
            spec.jvmArgs = getResolvedJvmArgs()
            if (getMaxHeapSize() != null) {
                spec.maxHeapSize = getMaxHeapSize()
            }
            spec.systemProperties getResolvedSystemProperties()
            spec.systemProperties getExtraSystemProperties()
            spec.enableAssertions = true
            spec.args = createArguments()
        }
    }

    @PackageScope
    Map<String, String> getExtraSystemProperties() {
        return [
            'com.asakusafw.batchapp.build.timestamp' : new Date().format('yyyy-MM-dd HH:mm:ss (z)'),
            'com.asakusafw.batchapp.build.java.version' : System.getProperty('java.version', '?')
        ]
    }

    @PackageScope
    List<String> createArguments() {
        List<String> results = []

        // project repository
        configureFiles(results, '--explore', getExplore())
        configureFiles(results, '--attach', getAttach())
        configureFiles(results, '--embed', getEmbed())
        configureFiles(results, '--external', getExternal())

        // batch class detection
        configureClasses(results, '--include', getResolvedInclude())
        configureClasses(results, '--exclude', getResolvedExclude())

        // custom compiler plug-ins
        configureClasses(results, '--dataModelProcessors', getResolvedCustomDataModelProcessors())
        configureClasses(results, '--externalPortProcessors', getResolvedCustomExternalPortProcessors())
        configureClasses(results, '--jobflowProcessors', getResolvedCustomJobflowProcessors())
        configureClasses(results, '--batchProcessors', getResolvedCustomBatchProcessors())
        configureClasses(results, '--participants', getResolvedCustomParticipants())

        // other options
        configureString(results, '--output', getOutputDirectory().getAbsolutePath())
        configureString(results, '--runtimeWorkingDirectory', getRuntimeWorkingDirectory())
        configureString(results, '--batchIdPrefix', getBatchIdPrefix())
        configureBoolean(results, '--failOnError', getFailOnError())
        getResolvedCompilerProperties().each { k, v ->
            results << '--property' << "${k}=${v}"
        }

        return results
    }

    private void configureBoolean(List<String> arguments, String key, boolean value) {
        if (value == false) {
            return
        }
        logger.debug("Asakusa compiler option: ${key}")
        arguments << key
    }

    private void configureString(List<String> arguments, String key, Object value) {
        if (value == null) {
            return
        }
        String s = String.valueOf(value)
        if (s.isEmpty()) {
            return
        }
        logger.debug("Asakusa compiler option: ${key}=${s}")
        arguments << key << s
    }

    private void configureFiles(List<String> arguments, String key, Object files) {
        FileCollection f = project.files(files)
        if (f.isEmpty()) {
            return
        }
        configureString(arguments, key, f.asPath)
    }

    private void configureClasses(List<String> arguments, String key, List<String> classes) {
        if (classes.isEmpty()) {
            return
        }
        configureString(arguments, key, classes.join(','))
    }
}
