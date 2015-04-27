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

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.graph.Group;

/**
 * Extra information for the broadcast sub-plan inputs/outputs.
 */
public class BroadcastInfo {

    private final Group formatInfo;

    /**
     * Creates a new instance.
     * @param formatInfo information of the broadcast data-set format
     */
    public BroadcastInfo(Group formatInfo) {
        this.formatInfo = formatInfo;
    }

    /**
     * Returns information of the broadcast data-set format.
     * @return the format information
     */
    public Group getFormatInfo() {
        return formatInfo;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Broadcast({0})", //$NON-NLS-1$
                formatInfo == null ? "N/A" : formatInfo); //$NON-NLS-1$
    }
}
