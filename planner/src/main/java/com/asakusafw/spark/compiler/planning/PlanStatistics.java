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
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.common.ComplexAttribute;
import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.spark.compiler.planning.SubPlanInfo.DriverType;
import com.asakusafw.spark.compiler.planning.SubPlanInputInfo.InputType;
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo.OutputType;

/**
 * Statistics of Spark execution plan.
 */
public class PlanStatistics implements ComplexAttribute {

    private final Map<DriverType, Integer> driverTypes;

    private final Map<InputType, Integer> inputTypes;

    private final Map<OutputType, Integer> outputTypes;

    /**
     * Creates a new instance.
     * @param driverTypes numbers of each driver type
     * @param inputTypes numbers of each input type
     * @param outputTypes numbers of each output type
     */
    public PlanStatistics(
            Map<DriverType, Integer> driverTypes,
            Map<InputType, Integer> inputTypes,
            Map<OutputType, Integer> outputTypes) {
        this.driverTypes = EnumUtil.freeze(driverTypes);
        this.inputTypes = EnumUtil.freeze(inputTypes);
        this.outputTypes = EnumUtil.freeze(outputTypes);
    }

    /**
     * Creates a new instance for the target plan.
     * @param plan the target plan
     * @return the created instance
     */
    public static PlanStatistics of(Plan plan) {
        Map<DriverType, Integer> driverTypes = new EnumMap<>(DriverType.class);
        Map<InputType, Integer> inputTypes = new EnumMap<>(InputType.class);
        Map<OutputType, Integer> outputTypes = new EnumMap<>(OutputType.class);
        for (SubPlan sub : plan.getElements()) {
            process(sub, driverTypes);
            for (SubPlan.Input port : sub.getInputs()) {
                process(port, inputTypes);
            }
            for (SubPlan.Output port : sub.getOutputs()) {
                process(port, outputTypes);
            }
        }
        return new PlanStatistics(driverTypes, inputTypes, outputTypes);
    }

    private static void process(SubPlan element, Map<DriverType, Integer> counters) {
        SubPlanInfo info = element.getAttribute(SubPlanInfo.class);
        if (info == null) {
            return;
        }
        increment(info.getDriverType(), counters);
    }

    private static void process(SubPlan.Input port, Map<InputType, Integer> counters) {
        SubPlanInputInfo info = port.getAttribute(SubPlanInputInfo.class);
        if (info == null) {
            return;
        }
        increment(info.getInputType(), counters);
    }

    private static void process(SubPlan.Output port, Map<OutputType, Integer> counters) {
        SubPlanOutputInfo info = port.getAttribute(SubPlanOutputInfo.class);
        if (info == null) {
            return;
        }
        increment(info.getOutputType(), counters);
    }

    private static <T> void increment(T member, Map<T, Integer> counters) {
        Integer count = counters.get(member);
        if (count == null) {
            counters.put(member, 1);
        } else {
            counters.put(member, count + 1);
        }
    }

    /**
     * Returns the numbers of each driver type.
     * @return the numbers of each driver type
     */
    public Map<DriverType, Integer> getDriverTypes() {
        return driverTypes;
    }

    /**
     * Returns the numbers of each input type.
     * @return the numbers of each input type
     */
    public Map<InputType, Integer> getInputTypes() {
        return inputTypes;
    }

    /**
     * Returns the numbers of each output type.
     * @return the numbers of each output type
     */
    public Map<OutputType, Integer> getOutputTypes() {
        return outputTypes;
    }

    @Override
    public Map<String, ?> getNestedAttributes() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("drivers", getDriverTypes()); //$NON-NLS-1$
        results.put("inputs", getInputTypes()); //$NON-NLS-1$
        results.put("outputs", getOutputTypes()); //$NON-NLS-1$
        return results;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Plan{0}", //$NON-NLS-1$
                getNestedAttributes());
    }
}
