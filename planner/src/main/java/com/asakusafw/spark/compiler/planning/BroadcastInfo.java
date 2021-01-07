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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.asakusafw.lang.compiler.common.ComplexAttribute;
import com.asakusafw.lang.compiler.model.graph.Group;

/**
 * Extra information for the broadcast sub-plan inputs/outputs.
 */
public class BroadcastInfo implements ComplexAttribute {

    private static final String UNKNOWN_LABEL = "unknown"; //$NON-NLS-1$

    private final String label;

    private final Group formatInfo;

    /**
     * Creates a new instance.
     * @param label label of this information (nullable)
     * @param formatInfo information of the broadcast data-set format
     */
    public BroadcastInfo(String label, Group formatInfo) {
        this.label = Objects.toString(label, UNKNOWN_LABEL);
        this.formatInfo = formatInfo;
    }

    /**
     * Creates a new instance.
     * @param formatInfo information of the broadcast data-set format
     */
    public BroadcastInfo(Group formatInfo) {
        this(null, formatInfo);
    }

    /**
     * Returns the label of this information.
     * @return the label (never null)
     */
    public String getLabel() {
        return label;
    }

    /**
     * Returns information of the broadcast data-set format.
     * @return the format information
     */
    public Group getFormatInfo() {
        return formatInfo;
    }

    @Override
    public Map<String, ?> toMap() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("label", Util.toLabel(getLabel())); //$NON-NLS-1$
        results.put("format", Util.toLabel(getFormatInfo())); //$NON-NLS-1$
        return results;
    }

    @Override
    public String toString() {
        return toMap().toString();
    }
}
