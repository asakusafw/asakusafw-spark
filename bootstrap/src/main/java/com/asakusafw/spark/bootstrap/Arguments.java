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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents program arguments.
 * @since 0.5.0
 */
public class Arguments {

    private final List<String> entries = new ArrayList<>();

    /**
     * Adds an entry.
     * @param entry the entry
     * @return this
     */
    public Arguments add(String entry) {
        entries.add(entry);
        return this;
    }

    /**
     * Adds a pair of entries.
     * @param e1 the first entry
     * @param e2 the second entry
     * @return this
     */
    public Arguments add(String e1, Object e2) {
        entries.add(e1);
        entries.add(String.valueOf(e2));
        return this;
    }

    /**
     * Adds a series of entries.
     * @param values the entries
     * @return this
     */
    public Arguments add(Iterable<String> values) {
        values.forEach(entries::add);
        return this;
    }

    /**
     * Returns the command line.
     * @param command the command path
     * @return the command line
     */
    public List<String> toCommandLine(Path command) {
        List<String> results = new ArrayList<>();
        results.add(command.toString());
        results.addAll(entries);
        return results;
    }

    @Override
    public String toString() {
        return entries.stream()
                .collect(Collectors.joining(", ", "Arguments(", ")"));
    }
}
