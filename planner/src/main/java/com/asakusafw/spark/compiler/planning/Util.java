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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CustomOperator;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.ExternalPort;
import com.asakusafw.lang.compiler.model.graph.FlowOperator;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorArgument;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorPort;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.Planning;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Tuple;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

final class Util {

    private static final String LABEL_NOT_AVAILABLE = "N/A"; //$NON-NLS-1$

    private static final OperatorKind[] TYPICAL_ORDER = {
        OperatorKind.MARKER,
        OperatorKind.CORE,
        OperatorKind.USER,
        OperatorKind.FLOW,
        OperatorKind.INPUT,
        OperatorKind.OUTPUT,
    };

    private static final long PRIME = 31L;

    private Util() {
        return;
    }

    /**
     * Collects IDs of user operators in the target sub-plan.
     * @param sub the target sub-plan
     * @return the operator IDs
     */
    static Set<OperatorId> collectOperatorIds(SubPlan sub) {
        Set<OperatorId> results = new HashSet<>();
        for (Operator operator : sub.getOperators()) {
            if (operator.getOperatorKind() == OperatorKind.USER) {
                results.add(OperatorId.of((UserOperator) operator));
            }
        }
        return results;
    }

    static Operator findMostTypical(Collection<? extends Operator> operators) {
        Operator current = null;
        for (Operator operator : operators) {
            if (current == null || isMoreTypical(operator, current)) {
                current = operator;
            }
        }
        return current;
    }

    static boolean isMoreTypical(Operator a, Operator b) {
        OperatorKind aKind = a.getOperatorKind();
        OperatorKind bKind = b.getOperatorKind();
        if (aKind == bKind) {
            return false;
        } else {
            for (OperatorKind target : TYPICAL_ORDER) {
                if (aKind == target || bKind == target) {
                    return bKind == target;
                }
            }
        }
        return false;
    }

    static String toLabel(Object value) {
        return Objects.toString(value, LABEL_NOT_AVAILABLE);
    }

    static String toOperatorLabel(Operator operator) {
        if (operator == null) {
            return LABEL_NOT_AVAILABLE;
        } else if (operator.getOperatorKind() == OperatorKind.USER) {
            UserOperator op = (UserOperator) operator;
            return MessageFormat.format(
                    "@{0}:{1}.{2}", //$NON-NLS-1$
                    op.getAnnotation().getDeclaringClass().getSimpleName(),
                    op.getMethod().getDeclaringClass().getSimpleName(),
                    op.getMethod().getName());
        } else {
            return operator.toString();
        }
    }

    static boolean isPrimaryInput(SubPlan.Input port) {
        PlanMarker marker = PlanMarkers.get(port.getOperator());
        return marker != null && marker != PlanMarker.BROADCAST;
    }

    static boolean isEffectiveInput(SubPlan.Input port) {
        PlanMarker marker = PlanMarkers.get(port.getOperator());
        return marker == PlanMarker.CHECKPOINT || marker == PlanMarker.GATHER || marker == PlanMarker.BROADCAST;
    }

    static <T> Map<T, String> computeIds(String prefix, List<T> sorted) {
        int digits = getNumberOfDigits(sorted.size());
        Map<T, String> results = new HashMap<>();
        for (T element : sorted) {
            String id = prefix + toStringWithLeadingZeros(results.size(), digits);
            results.put(element, id);
        }
        return results;
    }

    private static String toStringWithLeadingZeros(int value, int digits) {
        Arguments.require(value >= 0);
        StringBuilder buf = new StringBuilder(digits);
        for (int i = getNumberOfDigits(value); i < digits; i++) {
            buf.append('0');
        }
        buf.append(value);
        return buf.toString();
    }

    private static int getNumberOfDigits(int value) {
        Arguments.require(value >= 0);
        if (value < 10) {
            return 1;
        }
        int log = (int) Math.floor(Math.log10(value));
        return log + 1;
    }

    static <T, P> Map<T, String> computeIds(
            String prefix, Collection<? extends P> parents, Function<P, List<T>> sorter) {
        Map<T, String> results = new HashMap<>();
        for (P parent : parents) {
            List<T> sorted = sorter.apply(parent);
            Map<T, String> map = computeIds(prefix, sorted);
            map.forEach((element, label) -> {
                results.merge(element, label, (a, b) -> {
                    throw new IllegalStateException();
                });
            });
        }
        return results;
    }

    static List<SubPlan> sortElements(Plan plan) {
        List<SubPlan> results = new ArrayList<>();
        Graph<SubPlan> graph = Planning.toDependencyGraph(plan);
        while (graph.isEmpty() == false) {
            Set<SubPlan> heads = Graphs.collectTails(graph);
            List<SubPlan> sorted = sort(heads, Util::getDigest);
            graph.removeNodes(sorted);
            results.addAll(sorted);
        }
        return results;
    }

    static List<SubPlan.Input> sortInputs(SubPlan sub) {
        List<SubPlan.Input> primary = new ArrayList<>();
        List<SubPlan.Input> broadcast = new ArrayList<>();
        List<SubPlan.Input> dummy = new ArrayList<>();
        for (SubPlan.Input port : sub.getInputs()) {
            if (isEffectiveInput(port)) {
                if (isPrimaryInput(port)) {
                    primary.add(port);
                } else {
                    broadcast.add(port);
                }
            } else {
                dummy.add(port);
            }
        }
        List<SubPlan.Input> results = new ArrayList<>();
        results.addAll(sortPrimaryInputs(primary));
        results.addAll(sort(broadcast, Util::getDigest));
        results.addAll(sort(dummy, Util::getDigest));
        return results;
    }

    private static List<SubPlan.Input> sortPrimaryInputs(List<SubPlan.Input> primary) {
        if (primary.size() <= 1) {
            return primary;
        }
        Map<Operator, SubPlan.Input> map = primary.stream()
                .collect(Collectors.toMap(SubPlan.Input::getOperator, Function.identity()));
        List<Operator> candidates = primary.stream()
            .map(p -> p.getOperator())
            .peek(o -> Invariants.require(PlanMarkers.get(o) == PlanMarker.GATHER))
            .flatMap(o -> Operators.getSuccessors(o).stream())
            .distinct()
            .collect(Collectors.toList());
        Invariants.require(candidates.size() == 1);
        List<SubPlan.Input> results = candidates.get(0).getInputs().stream()
            .map(OperatorInput::getOpposites)
            .filter(x -> x.isEmpty() == false)
            .peek(x -> Invariants.require(x.size() == 1))
            .map(x -> x.iterator().next().getOwner())
            .filter(map::containsKey)
            .map(map::get)
            .collect(Collectors.toList());
        Invariants.require(results.size() == primary.size());
        return results;
    }

    static List<SubPlan.Output> sortOutputs(SubPlan sub) {
        return sort(sub.getOutputs(), Util::getDigest);
    }

    static <T> List<T> sort(Collection<? extends T> elements, ToLongFunction<? super T> digester) {
        return elements.stream()
                .map(e -> new Tuple<>(e, digester.applyAsLong(e)))
                .sorted(Comparator.comparingLong(Tuple::right))
                .map(Tuple::left)
                .collect(Collectors.toList());
    }

    static long getDigest(SubPlan element) {
        return getOperatorSetDigest(element.getOperators());
    }

    static long getDigest(SubPlan.Input element) {
        long result = 0;
        result = result * PRIME + getDigest(element.getOwner());
        result = result * PRIME + getConnectedSetDigest(element.getOperator().getOutput().getOpposites());
        return result;
    }

    static long getDigest(SubPlan.Output element) {
        long result = 1;
        result = result * PRIME + getDigest(element.getOwner());
        result = result * PRIME + getConnectedSetDigest(element.getOperator().getInput().getOpposites());
        return result;
    }

    private static long getConnectedSetDigest(Collection<? extends OperatorPort> opposites) {
        long result = 0;
        for (OperatorPort port : opposites) {
            long local = 0;
            local = local * PRIME + Objects.hashCode(port.getName());
            local = local * PRIME + Objects.hashCode(port.getDataType());
            local = local * PRIME + getDigest(port.getOwner());
            result += local;
        }
        return result;
    }

    static long getDigest(Operator operator) {
        switch (operator.getOperatorKind()) {
        case INPUT:
            return getOperatorDigest0((ExternalInput) operator);
        case OUTPUT:
            return getOperatorDigest0((ExternalOutput) operator);
        case USER:
            return getOperatorDigest0((UserOperator) operator);
        case CORE:
            return getOperatorDigest0((CoreOperator) operator);
        case FLOW:
            return getOperatorDigest0((FlowOperator) operator);
        case MARKER:
            return getOperatorDigest0((MarkerOperator) operator);
        case CUSTOM:
            return getOperatorDigest0((CustomOperator) operator);
        default:
            return operator.getOperatorKind().hashCode();
        }
    }

    private static long getOperatorDigest0(ExternalPort operator) {
        long result = getOperatorCommonDigest(operator);
        result = result * PRIME + Objects.hashCode(operator.getName());
        return result;
    }

    private static long getOperatorDigest0(UserOperator operator) {
        long result = getOperatorCommonDigest(operator);
        result = result * PRIME + Objects.hashCode(operator.getAnnotation());
        result = result * PRIME + Objects.hashCode(operator.getMethod());
        result = result * PRIME + Objects.hashCode(operator.getImplementationClass());
        return result;
    }

    private static long getOperatorDigest0(CoreOperator operator) {
        long result = getOperatorCommonDigest(operator);
        result = result * PRIME + Objects.hashCode(operator.getCoreOperatorKind());
        return result;
    }

    private static long getOperatorDigest0(FlowOperator operator) {
        long result = getOperatorCommonDigest(operator);
        result = result * PRIME + Objects.hashCode(operator.getDescriptionClass());
        result = result * PRIME + getOperatorSetDigest(operator.getOperatorGraph().getOperators());
        return result;
    }

    private static long getOperatorDigest0(MarkerOperator operator) {
        long result = getOperatorCommonDigest(operator);
        return result;
    }

    private static long getOperatorDigest0(CustomOperator operator) {
        long result = getOperatorCommonDigest(operator);
        result = result * PRIME + Objects.hashCode(operator.getCategory());
        return result;
    }

    private static long getOperatorCommonDigest(Operator operator) {
        long result = operator.getOperatorKind().hashCode();
        for (OperatorInput property : operator.getInputs()) {
            result = result * PRIME + Objects.hashCode(property.getPropertyKind());
            result = result * PRIME + Objects.hashCode(property.getName());
            result = result * PRIME + Objects.hashCode(property.getDataType());
            result = result * PRIME + Objects.hashCode(property.getGroup());
        }
        for (OperatorOutput property : operator.getOutputs()) {
            result = result * PRIME + Objects.hashCode(property.getPropertyKind());
            result = result * PRIME + Objects.hashCode(property.getName());
            result = result * PRIME + Objects.hashCode(property.getDataType());
        }
        for (OperatorArgument property : operator.getArguments()) {
            result = result * PRIME + Objects.hashCode(property.getPropertyKind());
            result = result * PRIME + Objects.hashCode(property.getName());
            result = result * PRIME + Objects.hashCode(property.getValue());
        }
        return result;
    }

    private static long getOperatorSetDigest(Collection<? extends Operator> operators) {
        long result = 0L;
        for (Operator operator : operators) {
            result += getDigest(operator);
        }
        return result;
    }
}
