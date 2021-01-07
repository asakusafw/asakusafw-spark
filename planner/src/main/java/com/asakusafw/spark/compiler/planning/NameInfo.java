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
package com.asakusafw.spark.compiler.planning;

import java.util.Objects;
import java.util.Optional;

import com.asakusafw.lang.compiler.common.AttributeContainer;
import com.asakusafw.lang.compiler.common.AttributeProvider;

/**
 * Naming information for planning elements.
 * @since 0.4.2
 */
public class NameInfo {

    private final String name;

    /**
     * Creates a new instance.
     * @param name the element name
     */
    public NameInfo(String name) {
        Objects.requireNonNull(name);
        this.name = name;
    }

    static void bind(AttributeContainer element, String name) {
        element.putAttribute(NameInfo.class, new NameInfo(name));
    }

    /**
     * Returns the name of the given element.
     * @param element the target element
     * @return the element name, or {@code empty} if it is not defined
     */
    public static String getName(AttributeProvider element) {
        return Optional.ofNullable(element.getAttribute(NameInfo.class))
                .map(NameInfo::getName)
                .get();
    }

    /**
     * Returns the element name.
     * @return the element name
     */
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }
}
