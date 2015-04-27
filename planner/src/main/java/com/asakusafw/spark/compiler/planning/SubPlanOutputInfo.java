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
import java.util.Collection;
import java.util.Set;

import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Extra information for {@link com.asakusafw.lang.compiler.planning.SubPlan.Output SubPlan.Output}.
 */
public class SubPlanOutputInfo {

    private final SubPlan.Output origin;

    private final OutputType outputType;

    private final Set<OutputOption> outputOptions;

    private final Group partitionInfo;

    private final Operator aggregationInfo;

    /**
     * Creates a new instance.
     * @param origin the original sub-plan output
     * @param outputType the output operation type
     * @param outputOptions the extra output options
     * @param partitionInfo the output partitioning information:
     *     it is only available for {@link OutputType#PARTITIONED} and {@link OutputType#AGGREGATED}
     * @param aggregationInfo the output pre-aggregation operator:
     *     it is only available for {@link OutputType#AGGREGATED}
     */
    public SubPlanOutputInfo(
            SubPlan.Output origin,
            OutputType outputType,
            Collection<OutputOption> outputOptions,
            Group partitionInfo,
            Operator aggregationInfo) {
        this.origin = origin;
        this.outputType = outputType;
        this.outputOptions = EnumUtil.freeze(outputOptions);
        this.partitionInfo = partitionInfo;
        this.aggregationInfo = aggregationInfo;
    }

    /**
     * Returns the original sub-plan output.
     * @return the original sub-plan output
     */
    public SubPlan.Output getOrigin() {
        return origin;
    }

    /**
     * Returns the output operation type.
     * @return the operation type
     */
    public OutputType getOutputType() {
        return outputType;
    }

    /**
     * Returns the extra output options.
     * @return the output options
     */
    public Set<OutputOption> getOutputOptions() {
        return outputOptions;
    }

    /**
     * Returns the partitioning information.
     * This is only available for {@link OutputType#PARTITIONED} and {@link OutputType#AGGREGATED}.
     * @return the partitioning information, or {@code null} if it is not defined
     */
    public Group getPartitionInfo() {
        return partitionInfo;
    }

    /**
     * Returns the output pre-aggregation operator.
     * This is only available for {@link OutputType#AGGREGATED}.
     * @return the output pre-aggregation operator, or {@code null} if it is not defined
     */
    public Operator getAggregationInfo() {
        return aggregationInfo;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "{0}{1}(partition={2}, aggregation={3})", //$NON-NLS-1$
                outputType,
                outputOptions,
                partitionInfo == null ? "N/A" : partitionInfo, //$NON-NLS-1$
                SubPlanInfo.toSimpleString(aggregationInfo));
    }

    /**
     * Represents an output operation type.
     */
    public enum OutputType {

        /**
         * No succeeding operations.
         * <ul>
         * <li> Partition Info: N/A </li>
         * <li> Aggregation Info: N/A </li>
         * </ul>
         */
        DISCARD,

        /**
         * Succeeding operation don't care the output organization.
         * <ul>
         * <li> Partition Info: N/A </li>
         * <li> Aggregation Info: N/A </li>
         * </ul>
         */
        DONT_CARE,

        /**
         * Optimizes output for the succeeding broadcast operation.
         * <ul>
         * <li> Partition Info: N/A </li>
         * <li> Aggregation Info: N/A </li>
         * </ul>
         */
        BROADCAST,

        /**
         * Output must be pre-partitioned for the succeeding grouping/aggregation operation.
         * <ul>
         * <li> Partition Info: available </li>
         * <li> Aggregation Info: N/A </li>
         * </ul>
         */
        PARTITIONED,

        /**
         * Output should be pre-aggregated for the succeeding aggregation operation.
         * <ul>
         * <li> Partition Info: available </li>
         * <li> Aggregation Info: available </li>
         * </ul>
         */
        AGGREGATED,
    }

    /**
     * Represents extra attributes for outputs.
     */
    public enum OutputOption {

        // no special members
    }
}
