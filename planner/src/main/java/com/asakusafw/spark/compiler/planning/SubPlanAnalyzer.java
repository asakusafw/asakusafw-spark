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

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizers;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputAttribute;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.spark.compiler.planning.SubPlanInfo.DriverOption;
import com.asakusafw.spark.compiler.planning.SubPlanInfo.DriverType;
import com.asakusafw.spark.compiler.planning.SubPlanInputInfo.InputOption;
import com.asakusafw.spark.compiler.planning.SubPlanInputInfo.InputType;
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo.OutputOption;
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo.OutputType;

/**
 * Provides helpful information for consequent code generation phase about {@link SubPlan}.
 */
public class SubPlanAnalyzer {

    private final PlanDetail detail;

    private final Map<Operator, OperatorClass> operatorClasses;

    private final GroupKeyUnifier groupKeys = new GroupKeyUnifier();

    private final Map<SubPlan, SubPlanInfo> analyzed = new HashMap<>();

    private final Map<SubPlan.Output, SubPlanOutputInfo> analyzedOutputs = new HashMap<>();

    private final Map<SubPlan.Input, SubPlanInputInfo> analyzedInputs = new HashMap<>();

    SubPlanAnalyzer(PlanDetail detail, Map<Operator, OperatorClass> operatorClasses) {
        this.detail = detail;
        this.operatorClasses = operatorClasses;
    }

    /**
     * Creates a new instance.
     * @param context the current context
     * @param detail the detail of the target plan
     * @param normalized the normalized operator graph, which is origin of the target plan
     * @return the created instance
     */
    public static SubPlanAnalyzer newInstance(
            PlanningContext context,
            PlanDetail detail,
            OperatorGraph normalized) {
        Map<Operator, OperatorClass> characteristics = OperatorCharacterizers.apply(
                context.getOptimizerContext(),
                context.getEstimator(),
                context.getClassifier(),
                normalized.getOperators(false));
        return new SubPlanAnalyzer(detail, characteristics);
    }

    /**
     * Analyzes a sub-plan.
     * @param sub the target sub-plan
     * @return the analyzed information
     */
    public SubPlanInfo analyze(SubPlan sub) {
        SubPlanInfo cached = analyzed.get(sub);
        if (cached != null) {
            return cached;
        }
        SubPlanInfo info = analyze0(sub);
        analyzed.put(info.getOrigin(), info);
        return info;
    }

    private SubPlanInfo analyze0(SubPlan sub) {
        Operator primaryOperator = getPrimaryOperator(sub);
        DriverType driverType;
        Set<DriverOption> driverOptions;
        if (primaryOperator == null) {
            driverType = DriverType.EXTRACT;
            driverOptions = Collections.emptySet();
        } else {
            OperatorClass operatorClass = getOperatorClass(primaryOperator);
            driverType = computeDriverType(operatorClass);
            driverOptions = computeDriverOptions(operatorClass);

            // EXTRACT type drivers don't have any primary operators even if it is unique
            if (driverType == DriverType.EXTRACT) {
                primaryOperator = null;
            }
        }
        return new SubPlanInfo(sub, driverType, driverOptions, primaryOperator);
    }

    private OperatorClass getOperatorClass(Operator operator) {
        Operator source = detail.getSource(operator);
        assert source != null;
        OperatorClass operatorClass = operatorClasses.get(source);
        assert operatorClass != null;
        return operatorClass;
    }

    private Operator getPrimaryOperator(SubPlan sub) {
        Set<Operator> candidates = new LinkedHashSet<>();
        for (SubPlan.Input port : sub.getInputs()) {
            if (SubPlanInfo.isPrimaryInput(port)) {
                for (Operator candidate : Operators.getSuccessors(port.getOperator())) {
                    // sub-plan's output must not be a primary operator
                    if (sub.findOutput(candidate) == null) {
                        candidates.add(candidate);
                    }
                }
            }
        }
        // primary operator must be unique
        if (candidates.isEmpty()) {
            return null;
        }
        // if we cannot determines the unique primary operator,
        // primary input type of each candidate must be 'RECORD'
        if (candidates.size() >= 2) {
            for (Operator operator : candidates) {
                OperatorClass operatorClass = getOperatorClass(operator);
                if (operatorClass.getPrimaryInputType() != OperatorClass.InputType.RECORD) {
                    throw new IllegalStateException();
                }
            }
            return null;
        }
        return candidates.iterator().next();
    }

    private static DriverType computeDriverType(OperatorClass operatorClass) {
        Operator operator = operatorClass.getOperator();
        if (operator.getOperatorKind() == OperatorKind.INPUT) {
            return DriverType.INPUT;
        } else if (operator.getOperatorKind() == OperatorKind.OUTPUT) {
            return DriverType.OUTPUT;
        }
        if (operatorClass.getPrimaryInputType() == OperatorClass.InputType.RECORD) {
            return DriverType.EXTRACT;
        } else if (operatorClass.getPrimaryInputType() == OperatorClass.InputType.GROUP) {
            Set<OperatorInput> primary = operatorClass.getPrimaryInputs();
            if (primary.size() == 1) {
                Set<InputAttribute> attributes = operatorClass.getAttributes(primary.iterator().next());
                if (attributes.contains(InputAttribute.AGGREATE)) {
                    return DriverType.AGGREGATE;
                }
            }
            return DriverType.COGROUP;
        } else {
            throw new IllegalStateException();
        }
    }

    private static Set<DriverOption> computeDriverOptions(OperatorClass operatorClass) {
        Set<OperatorInput> primary = operatorClass.getPrimaryInputs();
        if (primary.size() == 1) {
            Set<InputAttribute> attributes = operatorClass.getAttributes(primary.iterator().next());
            if (attributes.contains(InputAttribute.PARTIAL_REDUCTION)) {
                return Collections.singleton(DriverOption.PARTIAL);
            }
        }
        return Collections.emptySet();
    }

    private static PlanMarker getPlanMarker(SubPlan.Port port) {
        PlanMarker marker = PlanMarkers.get(port.getOperator());
        if (marker == null) {
            throw new IllegalStateException();
        }
        return marker;
    }

    /**
     * Analyzes input of sub-plan.
     * @param input the target input
     * @return the analyzed information
     */
    public SubPlanInputInfo analyze(SubPlan.Input input) {
        SubPlanInputInfo cached = analyzedInputs.get(input);
        if (cached != null) {
            return cached;
        }
        SubPlanInputInfo info = analyze0(input);
        analyzedInputs.put(info.getOrigin(), info);
        return info;
    }

    private SubPlanInputInfo analyze0(SubPlan.Input input) {
        InputType type = computeInputType(input);
        Set<InputOption> options = computeInputOptions(input, type);
        switch (type) {
        case VOID:
        case DONT_CARE:
        case PREPARE_EXTERNAL_OUTPUT:
        case BROADCAST:
            return new SubPlanInputInfo(input, type, options, null);
        case PARTITIONED:
            return new SubPlanInputInfo(input, type, options, computeInputGroup(input));
        default:
            throw new AssertionError(type);
        }
    }

    private InputType computeInputType(SubPlan.Input input) {
        if (input.getOpposites().isEmpty()) {
            return InputType.VOID;
        }
        OutputType type = computeRequiredOutputType(input);
        switch (type) {
        case DONT_CARE:
            return InputType.DONT_CARE;
        case BROADCAST:
            return InputType.BROADCAST;
        case PARTITIONED:
        case AGGREGATED:
            return InputType.PARTITIONED;
        case PREPARE_EXTERNAL_OUTPUT:
            return InputType.PREPARE_EXTERNAL_OUTPUT;
        default:
            throw new AssertionError(type);
        }
    }

    private Set<InputOption> computeInputOptions(SubPlan.Input input, InputType type) {
        Set<InputOption> results = EnumSet.noneOf(InputOption.class);
        switch (type) {
        case VOID:
        case DONT_CARE:
        case PREPARE_EXTERNAL_OUTPUT:
            results.add(InputOption.PRIMARY);
            break;
        case PARTITIONED:
            results.add(InputOption.PRIMARY);
            if (isSpillOut(input)) {
                results.add(InputOption.SPILL_OUT);
            }
            break;
        case BROADCAST:
            break;
        default:
            throw new AssertionError(type);
        }
        return results;
    }

    private boolean isSpillOut(SubPlan.Input input) {
        for (OperatorInput consumer : input.getOperator().getOutput().getOpposites()) {
            OperatorClass info = getOperatorClass(consumer.getOwner());
            OperatorInput resolved = info.getOperator().findInput(consumer.getName());
            if (info.getAttributes(resolved).contains(InputAttribute.ESCAPED)) {
                return true;
            }
        }
        return false;
    }

    private Group computeInputGroup(SubPlan.Input input) {
        Group result = groupKeys.get(input);
        if (result == null) {
            throw new IllegalStateException();
        }
        return result;
    }

    /**
     * Analyzes output of sub-plan.
     * @param output the target output
     * @return the analyzed information
     */
    public SubPlanOutputInfo analyze(SubPlan.Output output) {
        SubPlanOutputInfo cached = analyzedOutputs.get(output);
        if (cached != null) {
            return cached;
        }
        SubPlanOutputInfo info = analyze0(output);
        analyzedOutputs.put(info.getOrigin(), info);
        return info;
    }

    private SubPlanOutputInfo analyze0(SubPlan.Output output) {
        OutputType type = computeOutputType(output);
        Set<OutputOption> options = Collections.emptySet();
        switch (type) {
        case DISCARD:
        case DONT_CARE:
        case BROADCAST:
        case PREPARE_EXTERNAL_OUTPUT:
            return new SubPlanOutputInfo(output, type, options, null, null);
        case PARTITIONED:
            return new SubPlanOutputInfo(output, type, options, computeOutputGroup(output), null);
        case AGGREGATED:
            return new SubPlanOutputInfo(output, type, options,
                    computeOutputGroup(output), computeOutputAggregator(output));
        default:
            throw new AssertionError(type);
        }
    }

    private OutputType computeOutputType(SubPlan.Output output) {
        Set<? extends SubPlan.Input> downstreams = output.getOpposites();
        if (downstreams.isEmpty()) {
            return OutputType.DISCARD;
        }
        OutputType result = null;
        for (SubPlan.Input downstream : downstreams) {
            OutputType candidate = computeRequiredOutputType(downstream);
            if (result == null) {
                result = candidate;
            } else {
                if (candidate != result) {
                    throw new IllegalStateException();
                }
            }
        }
        assert result != null;
        return result;
    }

    private OutputType computeRequiredOutputType(SubPlan.Input downstream) {
        PlanMarker marker = getPlanMarker(downstream);
        if (marker == PlanMarker.BROADCAST) {
            return OutputType.BROADCAST;
        }
        SubPlanInfo info = analyze(downstream.getOwner());
        switch (info.getDriverType()) {
        case EXTRACT:
            return OutputType.DONT_CARE;
        case OUTPUT:
            return OutputType.PREPARE_EXTERNAL_OUTPUT;
        case COGROUP:
            return OutputType.PARTITIONED;
        case AGGREGATE:
            if (info.getDriverOptions().contains(DriverOption.PARTIAL)) {
                return OutputType.AGGREGATED;
            } else {
                return OutputType.PARTITIONED;
            }
        default:
            throw new AssertionError(info);
        }
    }

    private Group computeOutputGroup(SubPlan.Output output) {
        Group result = groupKeys.get(output);
        if (result == null) {
            throw new IllegalStateException();
        }
        return result;
    }

    private Operator computeOutputAggregator(SubPlan.Output output) {
        OperatorInput result = null;
        for (SubPlan.Input opposite : output.getOpposites()) {
            for (OperatorInput candidate : opposite.getOperator().getOutput().getOpposites()) {
                if (result == null) {
                    result = candidate;
                } else {
                    if (isCompatible(result, candidate) == false) {
                        throw new IllegalStateException();
                    }
                }
            }
        }
        assert result != null;
        return result.getOwner();
    }

    private boolean isCompatible(OperatorInput a, OperatorInput b) {
        if (a.getOwner().getSerialNumber() != b.getOwner().getSerialNumber()) {
            return false;
        }
        if (a.getName().equals(a.getName()) == false) {
            return false;
        }
        return true;
    }

    /**
     * Returns {@link BroadcastInfo} only if the target port represents a broadcast.
     * @param port the target port
     * @return the analyzed information, or {@code null} if the target port does not represents a broadcast
     */
    public BroadcastInfo analyzeBroadcast(SubPlan.Input port) {
        SubPlanInputInfo info = analyze(port);
        if (info.getInputType() != InputType.BROADCAST) {
            return null;
        }
        Operator typical = Util.findMostTypical(collectDescriptivePredecessors(port));
        String label = typical == null ? null : Util.toOperatorLabel(typical);
        return new BroadcastInfo(label, groupKeys.get(port));
    }

    /**
     * Returns {@link BroadcastInfo} only if the target port represents a broadcast.
     * @param port the target port
     * @return the analyzed information, or {@code null} if the target port does not represents a broadcast
     */
    public BroadcastInfo analyzeBroadcast(SubPlan.Output port) {
        SubPlanOutputInfo info = analyze(port);
        if (info.getOutputType() != OutputType.BROADCAST) {
            return null;
        }
        Operator typical = Util.findMostTypical(collectDescriptivePredecessors(port));
        String label = typical == null ? null : Util.toOperatorLabel(typical);
        return new BroadcastInfo(label, groupKeys.get(port));
    }

    private Set<Operator> collectDescriptivePredecessors(SubPlan.Output port) {
        Set<Operator> results = new LinkedHashSet<>();
        SubPlan sub = port.getOwner();
        for (Operator operator : Operators.getPredecessors(port.getOperator())) {
            if (sub.findInput(operator) != null) {
                continue;
            }
            results.add(operator);
        }
        return results;
    }

    private Set<Operator> collectDescriptivePredecessors(SubPlan.Input port) {
        Set<Operator> results = new LinkedHashSet<>();
        for (SubPlan.Output opposite : port.getOpposites()) {
            results.addAll(collectDescriptivePredecessors(opposite));
        }
        return results;
    }
}
