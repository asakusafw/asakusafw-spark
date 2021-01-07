/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.integration.spark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Optional;
import java.util.stream.Stream;

final class Util {

    private static final Path PATH_OWNED = Paths.get("src/integration-test/data");

    private static final Path PATH_INHERITED = Paths.get("build/integration-test/data");

    private Util() {
        return;
    }

    static Path data(String path) {
        // owned project templates
        Path owned = PATH_OWNED.resolve(path);
        if (Files.exists(owned)) {
            return owned;
        }

        // "./gradlew prepareIntegrationTest" copies project templates
        Path inherited = PATH_INHERITED.resolve(path);
        if (Files.exists(inherited)) {
            return inherited;
        }
        throw new IllegalStateException(MessageFormat.format(
                "missing template data \"{0}\"",
                path));
    }

    static Stream<String> lines(Path file) {
        if (Files.isRegularFile(file) == false) {
            return Stream.empty();
        }
        if (Optional.ofNullable(file.getFileName())
                .map(Path::toString)
                .map(it -> it.startsWith(".") && it.endsWith(".crc"))
                .orElse(false)) {
            return Stream.empty();
        }
        try {
            return Files.readAllLines(file, StandardCharsets.UTF_8).stream()
                    .filter(it -> it.isEmpty() == false);
        } catch (IOException e) {
            throw new IllegalStateException(MessageFormat.format(
                    "error occurred while reading: {0}",
                    file), e);
        }
    }
}
