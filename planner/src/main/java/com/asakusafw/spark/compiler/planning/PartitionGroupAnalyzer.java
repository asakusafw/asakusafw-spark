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

import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Analyzes sub-plan inputs/outputs and provides {@link PartitionGroupInfo}.
 * This requires following extra information for each sub-plan input:
 * <ul>
 * <li> {@link SizeInfo} </li>
 * <li> {@link SubPlanInputInfo} </li>
 * </ul>
 */
public class PartitionGroupAnalyzer {

    /**
     * Returns {@link PartitionGroupInfo} for the target sub-plan input.
     * @param port the target input port
     * @return the analyzed result, or {@code null} if the target port is not in any partition groups
     */
    public PartitionGroupInfo analyze(SubPlan.Input port) {
        PlanMarker marker = PlanMarkers.get(port.getOperator());
        if (marker != PlanMarker.GATHER) {
            return null;
        }
        // FIXME implement
        return new PartitionGroupInfo(PartitionGroupInfo.DataSize.UNKNOWN);
    }

    /**
     * Returns {@link PartitionGroupInfo} for the target sub-plan output.
     * @param port the target output port
     * @return the analyzed result, or {@code null} if the target port is not in any partition groups
     */
    public PartitionGroupInfo analyze(SubPlan.Output port) {
        PlanMarker marker = PlanMarkers.get(port.getOperator());
        if (marker != PlanMarker.GATHER) {
            return null;
        }
        // FIXME implement
        return new PartitionGroupInfo(PartitionGroupInfo.DataSize.UNKNOWN);
    }
}
