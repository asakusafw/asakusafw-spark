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
package com.asakusafw.spark.compiler.planning;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.common.ComplexAttribute;
import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Extra information for {@link com.asakusafw.lang.compiler.planning.SubPlan.Input SubPlan.Input}.
 */
public class SubPlanInputInfo implements ComplexAttribute {

    private final SubPlan.Input origin;

    private final InputType inputType;

    private final Set<InputOption> inputOptions;

    private final Group partitionInfo;

    /**
     * Creates a new instance.
     * @param origin the original sub-plan input
     * @param inputType the input operation type
     * @param inputOptions the extra input options
     * @param partitionInfo the input partitioning information:
     *     it is only available for {@link InputType#PARTITIONED}
     */
    public SubPlanInputInfo(
            SubPlan.Input origin,
            InputType inputType,
            Collection<InputOption> inputOptions,
            Group partitionInfo) {
        this.origin = origin;
        this.inputType = inputType;
        this.inputOptions = EnumUtil.freeze(inputOptions);
        this.partitionInfo = partitionInfo;
    }

    /**
     * Returns the original sub-plan input.
     * @return the original sub-plan input
     */
    public SubPlan.Input getOrigin() {
        return origin;
    }

    /**
     * Returns the input operation type.
     * @return the operation type
     */
    public InputType getInputType() {
        return inputType;
    }

    /**
     * Returns the extra input options.
     * @return the input options
     */
    public Set<InputOption> getInputOptions() {
        return inputOptions;
    }

    /**
     * Returns the partitioning information.
     * This is only available for {@link InputType#PARTITIONED}.
     * @return the partitioning information, or {@code null} if it is not defined
     */
    public Group getPartitionInfo() {
        return partitionInfo;
    }

    @Override
    public Map<String, ?> toMap() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("type", getInputType()); //$NON-NLS-1$
        results.put("options", getInputOptions()); //$NON-NLS-1$
        results.put("partition", Util.toLabel(getPartitionInfo())); //$NON-NLS-1$
        return results;
    }

    @Override
    public String toString() {
        return toMap().toString();
    }

    /**
     * Represents an input operation type.
     */
    public enum InputType {

        /**
         * No precedent operations.
         * <ul>
         * <li> Partition Info: N/A </li>
         * </ul>
         */
        VOID,

        /**
         * Succeeding operation don't care the input organization.
         * <ul>
         * <li> Partition Info: N/A </li>
         * </ul>
         */
        DONT_CARE,

        /**
         * Optimizes input for the succeeding broadcast operation.
         * <ul>
         * <li> Partition Info: N/A </li>
         * </ul>
         */
        BROADCAST,

        /**
         * Input must be pre-partitioned for the succeeding grouping/aggregation operation.
         * <ul>
         * <li> Partition Info: available </li>
         * </ul>
         */
        PARTITIONED,

        /**
         * Prepares for external outputs.
         * <ul>
         * <li> Partition Info: N/A </li>
         * </ul>
         */
        PREPARE_EXTERNAL_OUTPUT,
    }

    /**
     * Represents extra attributes for inputs.
     * @since 0.1.0
     * @version 0.3.0
     */
    public enum InputOption {

        /**
         * Represents whether the target input is a primary one or not.
         */
        PRIMARY,

        /**
         * Each input group data should be able to spill out of the heap.
         * @since 0.3.0
         */
        SPILL_OUT,
    }
}
