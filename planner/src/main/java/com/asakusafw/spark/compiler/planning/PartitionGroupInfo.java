/**
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
package com.asakusafw.spark.compiler.planning;

import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.common.ComplexAttribute;

/**
 * Extra information for the partitioned sub-plan inputs/outputs.
 */
public class PartitionGroupInfo implements ComplexAttribute {

    private final DataSize dataSize;

    /**
     * Creates a new instance.
     * @param dataSize the data size scale
     */
    public PartitionGroupInfo(DataSize dataSize) {
        this.dataSize = dataSize;
    }

    /**
     * Returns the data size scale of the target partition group.
     * @return the data size scale
     */
    public DataSize getDataSize() {
        return dataSize;
    }

    @Override
    public Map<String, ?> toMap() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("dataSize", Util.toLabel(getDataSize())); //$NON-NLS-1$
        return results;
    }

    @Override
    public String toString() {
        return toMap().toString();
    }

    /**
     * Represents data size scale for {@link PartitionGroupInfo}.
     */
    public enum DataSize {

        /**
         * Tiny data-sets.
         */
        TINY,

        /**
         * Small data-sets.
         */
        SMALL,

        /**
         * Regular data-sets.
         */
        REGULAR,

        /**
         * Large data-sets.
         */
        LARGE,

        /**
         * Huge data-sets.
         */
        HUGE,

        /**
         * Unknown data-sets.
         */
        UNKNOWN,
    }
}
