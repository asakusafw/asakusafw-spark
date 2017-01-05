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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.common.ComplexAttribute;
import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Extra information for {@link SubPlan}.
 */
public class SubPlanInfo implements ComplexAttribute {

    private final SubPlan origin;

    private final DriverType driverType;

    private final Set<DriverOption> driverOptions;

    private final Operator primaryOperator;

    private final Set<SubPlan.Input> primaryInputs;

    private final Set<SubPlan.Input> secondaryInputs;

    /**
     * Creates a new instance.
     * @param origin the original sub-plan
     * @param driverType the execution driver type
     * @param driverOptions the extra driver options
     * @param primaryOperator the primary operator of the target sub-plan,
     *     it is the {@literal "head"} operator which non-broadcast inputs are directly connected to
     *     the target sub-plan's inputs (this becomes {@code null} for {@link DriverType#EXTRACT} kind drivers)
     */
    public SubPlanInfo(
            SubPlan origin,
            DriverType driverType,
            Collection<DriverOption> driverOptions,
            Operator primaryOperator) {
        this.origin = origin;
        this.driverType = driverType;
        this.driverOptions = EnumUtil.freeze(driverOptions);
        this.primaryOperator = primaryOperator;
        this.primaryInputs = collectInputs(origin.getInputs(), true);
        this.secondaryInputs = collectInputs(origin.getInputs(), false);
    }

    private Set<SubPlan.Input> collectInputs(Set<? extends SubPlan.Input> inputs, boolean primary) {
        Set<SubPlan.Input> results = new LinkedHashSet<>();
        for (SubPlan.Input port : inputs) {
            if (isPrimaryInput(port) == primary) {
                results.add(port);
            }
        }
        return Collections.unmodifiableSet(results);
    }

    /**
     * Returns whether the target is a primary input.
     * @param port the target input
     * @return {@code true} if the target is a primary input, otherwise {@code false}
     */
    public static boolean isPrimaryInput(SubPlan.Input port) {
        PlanMarker marker = PlanMarkers.get(port.getOperator());
        return marker != null && marker != PlanMarker.BROADCAST;
    }

    /**
     * Returns the original sub-plan.
     * @return the original sub-plan
     */
    public SubPlan getOrigin() {
        return origin;
    }

    /**
     * Returns the execution driver type.
     * @return the driver type
     */
    public DriverType getDriverType() {
        return driverType;
    }

    /**
     * Returns the extra driver options.
     * @return the driver options
     */
    public Set<DriverOption> getDriverOptions() {
        return driverOptions;
    }

    /**
     * Returns the primary operator of the target sub-plan.
     * It is the operator whose non-broadcast inputs directly come from the target sub-plan's inputs,
     * and it dominates the target sub-plan. That is, if the operator exists, each
     * {@link #getPrimaryInputs() primary input} is exclusively connected to it.
     * Note that, {@link DriverType#EXTRACT} type sub-plans may not be determined the <em>unique</em> primary operator,
     * and then this method always returns {@code null} in such a case.
     * @return the primary operator, or {@code null} if the driver is {@link DriverType#EXTRACT}
     */
    public Operator getPrimaryOperator() {
        return primaryOperator;
    }

    /**
     * Returns the primary sub-plan inputs, which are all non-broadcast inputs of the target sub-plan.
     * Sub-plans must have at least one primary input.
     * @return the primary inputs
     */
    public Set<SubPlan.Input> getPrimaryInputs() {
        return primaryInputs;
    }

    /**
     * Returns the secondary sub-plan inputs, which are all broadcast inputs of the target sub-plan.
     * Sub-plans may not have any secondary inputs.
     * @return the secondary inputs
     */
    public Set<SubPlan.Input> getSecondaryInputs() {
        return secondaryInputs;
    }

    /**
     * Returns the label of this information.
     * @return the label (never null)
     */
    public String getLabel() {
        Operator typical = getTypicalOperator();
        if (typical == null) {
            return "Id"; //$NON-NLS-1$
        } else {
            return Util.toOperatorLabel(typical);
        }
    }

    @Override
    public Map<String, ?> toMap() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("type", getDriverType()); //$NON-NLS-1$
        results.put("options", getDriverOptions()); //$NON-NLS-1$
        results.put("label", getLabel()); //$NON-NLS-1$
        return results;
    }

    @Override
    public String toString() {
        return toMap().toString();
    }

    private Operator getTypicalOperator() {
        if (primaryOperator != null) {
            return primaryOperator;
        }
        List<Operator> candidates = new ArrayList<>();
        for (SubPlan.Input input : getPrimaryInputs()) {
            for (Operator operator : Operators.getSuccessors(input.getOperator())) {
                if (origin.findOutput(operator) != null) {
                    continue;
                }
                candidates.add(operator);
            }
        }
        return Util.findMostTypical(candidates);
    }

    /**
     * Represents an execution driver type on the Spark execution.
     */
    public enum DriverType {

        /**
         * Drivers which process the external inputs.
         * <ul>
         * <li> Driver options: N/A </li>
         * <li> Primary operator: {@link ExternalInput} </li>
         * <li> Primary inputs: {@code 1} </li>
         * <li> Secondary inputs: {@code 0..} </li>
         * </ul>
         */
        INPUT,

        /**
         * Drivers which process external outputs.
         * <ul>
         * <li> Driver options: N/A </li>
         * <li> Primary operator: {@link ExternalOutput} </li>
         * <li> Primary inputs: {@code 1} </li>
         * <li> Secondary inputs: {@code 0} </li>
         * </ul>
         */
        OUTPUT,

        /**
         * Drivers which process extract kind operators.
         * <ul>
         * <li> Driver options: N/A </li>
         * <li> Primary operator: N/A (always {@code null}) </li>
         * <li> Primary inputs: {@code 1} </li>
         * <li> Secondary inputs: {@code 0..} </li>
         * </ul>
         */
        EXTRACT,

        /**
         * Drivers which process co-group kind operators.
         * <ul>
         * <li> Driver options: N/A </li>
         * <li> Primary operator: {@link UserOperator} </li>
         * <li> Primary inputs: {@code 1..} </li>
         * <li> Secondary inputs: {@code 0..} </li>
         * </ul>
         */
        COGROUP,

        /**
         * Drivers which process aggregation kind operators.
         * <ul>
         * <li> Driver options: {@link DriverOption#PARTIAL} </li>
         * <li> Primary operator: {@link UserOperator} </li>
         * <li> Primary inputs: {@code 1} </li>
         * <li> Secondary inputs: {@code 0..} </li>
         * </ul>
         */
        AGGREGATE,
        ;
    }

    /**
     * Represents extra options for execution drivers.
     * @see DriverType
     */
    public enum DriverOption {

        /**
         * The target driver allows partial aggregation for the current sub-plan's input.
         */
        PARTIAL,
    }
}
