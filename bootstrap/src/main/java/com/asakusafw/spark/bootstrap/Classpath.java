/**
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
package com.asakusafw.spark.bootstrap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a classpath of {@link SparkBootstrap}.
 * @since 0.5.0
 */
public class Classpath {

    private final List<Path> entries = new ArrayList<>();

    /**
     * Adds an entry into this classpath.
     * @param entry the target file or directory
     * @param mandatory {@code true} if it is mandatory, otherwise {@code false}
     * @return this
     * @throws IllegalStateException if the mandatory entry is not found
     */
    public Classpath add(Path entry, boolean mandatory) {
        if (Files.exists(entry)) {
            entries.add(entry);
        } else if (mandatory) {
            throw new IllegalStateException(MessageFormat.format(
                    "classpath entry must exist: {0}",
                    entry));
        }
        return this;
    }

    /**
     * Adds entries in the given directory into this classpath.
     * @param directory the target directory
     * @param mandatory {@code true} if it is mandatory, otherwise {@code false}
     * @return this
     * @throws IllegalStateException if the mandatory directory is not found
     */
    public Classpath addEntries(Path directory, boolean mandatory) {
        if (Files.isDirectory(directory)) {
            try {
                Files.list(directory).forEach(entries::add);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (mandatory) {
            throw new IllegalStateException(MessageFormat.format(
                    "classpath entry must exist: {0}",
                    directory));
        }
        return this;
    }

    /**
     * Returns the entries.
     * @return the entries
     */
    public List<Path> getEntries() {
        return entries;
    }

    @Override
    public String toString() {
        return entries.stream()
                .map(Path::toString)
                .collect(Collectors.joining(", ", "Classpath(", ")"));
    }
}
