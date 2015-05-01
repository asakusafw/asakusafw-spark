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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.common.AttributeContainer;
import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizers;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriters;
import com.asakusafw.lang.compiler.optimizer.adapter.OptimizerContextAdapter;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass;
import com.asakusafw.lang.compiler.planning.OperatorEquivalence;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanAssembler;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.Planning;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.compiler.planning.util.GraphStatistics;
import com.asakusafw.spark.compiler.planning.PlanningContext.Option;
import com.asakusafw.spark.compiler.planning.SubPlanInputInfo.InputType;
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo.OutputType;
import com.asakusafw.utils.graph.Graph;

/**
 * Utilities for execution planning on Spark compiler.
 * The elements in the created plan will have the following {@link AttributeContainer#getAttribute(Class) attributes}:
 * <ul>
 * <li> {@link SubPlanInfo} - for {@link SubPlan} </li>
 * <li> {@link SubPlanInputInfo} - for {@link com.asakusafw.lang.compiler.planning.SubPlan.Input} </li>
 * <li> {@link SubPlanOutputInfo} - for {@link com.asakusafw.lang.compiler.planning.SubPlan.Output} </li>
 * <li> {@link BroadcastInfo} -
 *      for {@link com.asakusafw.lang.compiler.planning.SubPlan.Port} ({@code BROADCAST} ports only)
 * </li>
 * </ul>
 */
public final class SparkPlanning {

    static final Set<Option> DEFAULT_OPTIONS = EnumUtil.freeze(new Option[] {
            Option.UNIFY_SUBPLAN_IO,
            Option.CHECKPOINT_BEFORE_EXTERNAL_OUTPUTS,
            Option.GRAPH_STATISTICS,
            Option.PLAN_STATISTICS,
    });

    private SparkPlanning() {
        return;
    }

    /**
     * Builds an execution plan for the target jobflow.
     * Note that, this does not modifies the operator graph in the target jobflow.
     * @param parent the current jobflow processing context
     * @param jobflow the target jobflow
     * @return the detail of created plan
     */
    public static PlanDetail plan(JobflowProcessor.Context parent, Jobflow jobflow) {
        return plan(parent, jobflow, jobflow.getOperatorGraph().copy());
    }

    /**
     * Builds an execution plan for the target operator graph.
     * Note that, the target operator graph will be modified in this invocation.
     * @param parent the current jobflow processing context
     * @param jobflow the target jobflow information
     * @param operators the target operator graph
     * @return the detail of created plan
     */
    public static PlanDetail plan(JobflowProcessor.Context parent, JobflowInfo jobflow, OperatorGraph operators) {
        PlanningContext context = createContext(parent, jobflow, DEFAULT_OPTIONS);
        return plan(context, operators);
    }

    /**
     * Creates a new planner context.
     * @param parent the the current jobflow processing context
     * @param jobflow the target jobflow information
     * @param options the planning options
     * @return the created context
     */
    public static PlanningContext createContext(
            JobflowProcessor.Context parent,
            JobflowInfo jobflow,
            Collection<PlanningContext.Option> options) {
        PlanningContext context = new PlanningContext(
                new OptimizerContextAdapter(parent, jobflow.getFlowId()),
                options);
        return context;
    }

    /**
     * Builds an execution plan for the target operator graph.
     * Note that, the target operator graph will be modified in this invocation.
     * @param context the current context
     * @param operators the target operator graph
     * @return the detail of created plan
     */
    public static PlanDetail plan(PlanningContext context, OperatorGraph operators) {
        prepareOperatorGraph(context, operators);
        PlanDetail result = createPlan(context, operators);
        return result;
    }

    /**
     * Makes the target operator graph suitable for execution planning.
     * @param context the current context
     * @param graph the target operator graph
     */
    static void prepareOperatorGraph(PlanningContext context, OperatorGraph graph) {
        Planning.normalize(graph);
        Planning.removeDeadFlow(graph);
        OperatorRewriters.apply(
                context.getOptimizerContext(),
                context.getEstimator(),
                context.getRewriter(),
                graph);
        insertPlanMarkers(context, graph);
        Planning.simplifyTerminators(graph);
    }

    static void insertPlanMarkers(PlanningContext context, OperatorGraph graph) {
        rewriteCheckpointOperators(graph);
        Map<Operator, OperatorClass> characteristics = OperatorCharacterizers.apply(
                context.getOptimizerContext(),
                context.getEstimator(),
                context.getClassifier(),
                graph.getOperators(false));
        for (OperatorClass info : characteristics.values()) {
            insertPlanMarkerForPreparingGroup(info);
            insertPlanMarkerForPreparingBroadcast(info);
            if (context.getOptions().contains(Option.CHECKPOINT_AFTER_EXTERNAL_INPUTS)) {
                insertPlanMarkerForEnsuringExternalInput(info);
            }
            if (context.getOptions().contains(Option.CHECKPOINT_BEFORE_EXTERNAL_OUTPUTS)) {
                insertPlanMarkerForPreparingExternalOutput(info);
            }
        }
        graph.rebuild();
    }

    private static void rewriteCheckpointOperators(OperatorGraph graph) {
        for (Operator operator : graph.getOperators(true)) {
            if (operator.getOperatorKind() != OperatorKind.CORE
                    || ((CoreOperator) operator).getCoreOperatorKind() != CoreOperatorKind.CHECKPOINT) {
                continue;
            }
            PlanMarkers.insert(PlanMarker.CHECKPOINT, operator.getInputs().get(0));
            Operators.remove(operator);
            graph.remove(operator);
        }
    }

    private static void insertPlanMarkerForPreparingGroup(OperatorClass info) {
        if (info.getPrimaryInputType() != OperatorClass.InputType.GROUP) {
            return;
        }
        for (OperatorInput port : info.getPrimaryInputs()) {
            if (isEmpty(port) == false) {
                boolean partial = info.getAttributes(port).contains(OperatorClass.InputAttribute.PARTIAL_REDUCTION);
                EdgeInfo edge = new EdgeInfo(
                        port.getDataType(),
                        port.getGroup(),
                        partial ? info.getOperator() : null);
                Operators.insert(MarkerOperator.builder(port.getDataType())
                        .attribute(PlanMarker.class, PlanMarker.GATHER)
                        .attribute(EdgeInfo.class, edge)
                        .build(), port);
            }
        }
    }

    private static void insertPlanMarkerForPreparingBroadcast(OperatorClass info) {
        for (OperatorInput port : info.getSecondaryInputs()) {
            if (isEmpty(port) == false) {
                EdgeInfo edge = new EdgeInfo(port.getDataType(), port.getGroup(), null);
                Operators.insert(MarkerOperator.builder(port.getDataType())
                        .attribute(PlanMarker.class, PlanMarker.BROADCAST)
                        .attribute(EdgeInfo.class, edge)
                        .build(), port);
            }
        }
    }

    private static boolean isEmpty(OperatorInput port) {
        for (Operator upstream : Operators.getPredecessors(Collections.singleton(port))) {
            PlanMarker marker = PlanMarkers.get(upstream);
            if (marker != PlanMarker.BEGIN) {
                return false;
            }
        }
        return true;
    }

    private static void insertPlanMarkerForEnsuringExternalInput(OperatorClass info) {
        if (info.getOperator().getOperatorKind() != OperatorKind.INPUT) {
            return;
        }
        ExternalInput input = (ExternalInput) info.getOperator();
        PlanMarkers.insert(PlanMarker.CHECKPOINT, input.getOperatorPort());
    }

    private static void insertPlanMarkerForPreparingExternalOutput(OperatorClass info) {
        if (info.getOperator().getOperatorKind() != OperatorKind.OUTPUT) {
            return;
        }
        ExternalOutput output = (ExternalOutput) info.getOperator();
        PlanMarkers.insert(PlanMarker.CHECKPOINT, output.getOperatorPort());
    }

    static PlanDetail createPlan(PlanningContext context, OperatorGraph normalized) {
        PlanDetail primitive = createPrimitivePlan(normalized);
        PlanDetail unified = unifySubPlans(context, primitive);

        SubPlanAnalyzer analyzer = SubPlanAnalyzer.newInstance(context, unified, normalized);
        decoratePlan(context, unified.getPlan(), analyzer);
        return unified;
    }

    private static PlanDetail createPrimitivePlan(OperatorGraph graph) {
        return Planning.createPrimitivePlan(graph);
    }

    private static PlanDetail unifySubPlans(PlanningContext context, PlanDetail primitive) {
        PlanAssembler assembler = Planning.startAssemblePlan(primitive)
                .withTrivialOutputElimination(true)
                .withRedundantOutputElimination(true)
                .withDuplicateCheckpointElimination(true)
                .withUnionPushDown(true)
                .withSortResult(true);
        if (context.getOptions().contains(Option.UNIFY_SUBPLAN_IO)) {
            assembler.withCustomEquivalence(new CustomEquivalence());
        }
        Collection<SubPlanGroup> groups = classify(primitive);
        for (SubPlanGroup group : groups) {
            assembler.add(group.elements);
        }
        PlanDetail plan = assembler.build();
        return plan;
    }

    private static Collection<SubPlanGroup> classify(PlanDetail primitive) {
        List<SubPlanGroup> groups = new ArrayList<>();
        for (SubPlan subplan : primitive.getPlan().getElements()) {
            groups.add(SubPlanGroup.of(primitive, subplan));
        }
        groups = combineGroups(groups);

        Graph<SubPlan> dependencies = Planning.toDependencyGraph(primitive.getPlan());
        groups = splitGroups(dependencies, groups);

        return groups;
    }

    private static List<SubPlanGroup> combineGroups(List<SubPlanGroup> groups) {
        Map<Set<Operator>, SubPlanGroup> map = new LinkedHashMap<>();
        for (SubPlanGroup group : groups) {
            SubPlanGroup buddy = map.get(group.commonSources);
            if (buddy == null) {
                map.put(group.commonSources, group);
            } else {
                buddy.elements.addAll(group.elements);
            }
        }
        return new ArrayList<>(map.values());
    }

    private static List<SubPlanGroup> splitGroups(Graph<SubPlan> dependencies, List<SubPlanGroup> groups) {
        List<SubPlanGroup> results = new ArrayList<>(groups);
        List<SubPlanGroup> purged = new ArrayList<>();
        while (true) {
            for (SubPlanGroup group : results) {
                SubPlanGroup g = splitGroup(dependencies, group);
                if (g != null) {
                    purged.add(g);
                }
            }
            if (purged.isEmpty()) {
                break;
            } else {
                results.addAll(purged);
                purged.clear();
            }
        }
        return results;
    }

    private static SubPlanGroup splitGroup(Graph<SubPlan> dependencies, SubPlanGroup group) {
        assert group.elements.isEmpty() == false;
        if (group.elements.size() <= 1) {
            return null;
        }
        Set<SubPlan> blockers = computeBlockers(dependencies, group);
        List<SubPlan> purged = new ArrayList<>();
        for (Iterator<SubPlan> iter = group.elements.iterator(); iter.hasNext();) {
            SubPlan element = iter.next();
            if (blockers.contains(element)) {
                purged.add(element);
                iter.remove();
            }
        }
        assert group.elements.isEmpty() == false;
        if (purged.isEmpty()) {
            return null;
        }
        SubPlanGroup result = new SubPlanGroup(group.commonSources);
        result.elements.addAll(purged);
        return result;
    }

    private static Set<SubPlan> computeBlockers(Graph<SubPlan> dependencies, SubPlanGroup group) {
        Set<SubPlan> saw = new HashSet<>();
        LinkedList<SubPlan> work = new LinkedList<>(group.elements);
        while (work.isEmpty() == false) {
            SubPlan first = work.removeFirst();
            for (SubPlan blocker : dependencies.getConnected(first)) {
                if (saw.contains(blocker)) {
                    continue;
                }
                saw.add(blocker);
                work.add(blocker);
            }
        }
        return saw;
    }

    private static void decoratePlan(PlanningContext context, Plan plan, SubPlanAnalyzer analyzer) {
        attachCoreInfo(plan, analyzer);
        attachBroadcastInfo(plan, analyzer);
        if (context.getOptions().contains(Option.GRAPH_STATISTICS)) {
            attachGraphStatistics(plan);
        }
        if (context.getOptions().contains(Option.PLAN_STATISTICS)) {
            attachPlanStatistics(plan);
        }
    }

    private static void attachCoreInfo(Plan plan, SubPlanAnalyzer analyzer) {
        for (SubPlan sub : plan.getElements()) {
            SubPlanInfo info = analyzer.analyze(sub);
            assert info.getOrigin() == sub;
            sub.putAttribute(SubPlanInfo.class, info);
        }
        for (SubPlan sub : plan.getElements()) {
            for (SubPlan.Input input : sub.getInputs()) {
                SubPlanInputInfo info = analyzer.analyze(input);
                assert info.getOrigin() == input;
                input.putAttribute(SubPlanInputInfo.class, info);
            }
            for (SubPlan.Output output : sub.getOutputs()) {
                SubPlanOutputInfo info = analyzer.analyze(output);
                assert info.getOrigin() == output;
                output.putAttribute(SubPlanOutputInfo.class, info);
            }
        }
    }

    private static void attachBroadcastInfo(Plan plan, SubPlanAnalyzer analyzer) {
        GroupKeyUnifier unifier = new GroupKeyUnifier();
        for (SubPlan sub : plan.getElements()) {
            for (SubPlan.Input input : sub.getInputs()) {
                SubPlanInputInfo core = analyzer.analyze(input);
                if (core.getInputType() == InputType.BROADCAST) {
                    BroadcastInfo info = new BroadcastInfo(unifier.get(input));
                    input.putAttribute(BroadcastInfo.class, info);
                }
            }
            for (SubPlan.Output output : sub.getOutputs()) {
                SubPlanOutputInfo core = analyzer.analyze(output);
                if (core.getOutputType() == OutputType.BROADCAST) {
                    BroadcastInfo info = new BroadcastInfo(unifier.get(output));
                    output.putAttribute(BroadcastInfo.class, info);
                }
            }
        }
    }

    private static void attachGraphStatistics(Plan plan) {
        plan.putAttribute(GraphStatistics.class, GraphStatistics.of(Planning.toDependencyGraph(plan)));
        for (SubPlan sub : plan.getElements()) {
            sub.putAttribute(GraphStatistics.class, GraphStatistics.of(Planning.toDependencyGraph(sub)));
        }
    }

    private static void attachPlanStatistics(Plan plan) {
        plan.putAttribute(PlanStatistics.class, PlanStatistics.of(plan));
    }

    private static class SubPlanGroup {

        final Set<Operator> commonSources;

        final List<SubPlan> elements = new LinkedList<>();

        public SubPlanGroup(Set<Operator> commonSources) {
            this.commonSources = Collections.unmodifiableSet(commonSources);
        }

        public static SubPlanGroup of(PlanDetail detail, SubPlan source) {
            Set<Operator> sources = new LinkedHashSet<>();
            for (SubPlan.Input input : source.getInputs()) {
                PlanMarker marker = PlanMarkers.get(input.getOperator());
                if (marker != PlanMarker.BROADCAST) {
                    sources.add(detail.getSource(input.getOperator()));
                }
            }
            assert sources.isEmpty() == false;
            SubPlanGroup group = new SubPlanGroup(sources);
            group.elements.add(source);
            return group;
        }
    }

    private static class CustomEquivalence implements OperatorEquivalence {

        public CustomEquivalence() {
            return;
        }

        @Override
        public Object extract(SubPlan owner, Operator operator) {
            SubPlan.Input input = owner.findInput(operator);
            if (input != null) {
                return extract(input);
            }
            SubPlan.Output output = owner.findOutput(operator);
            if (output != null) {
                return extract(output);
            }
            return PlanAssembler.DEFAULT_EQUIVALENCE.extract(owner, operator);
        }

        private Object extract(SubPlan.Input port) {
            MarkerOperator operator = port.getOperator();
            PlanMarker marker = PlanMarkers.get(operator);
            assert marker != null;
            switch (marker) {
            case CHECKPOINT:
                return operator.getDataType();
            case BROADCAST:
                assert operator.getAttribute(EdgeInfo.class) != null;
                return operator.getAttribute(EdgeInfo.class);
            default:
                return PlanAssembler.DEFAULT_EQUIVALENCE.extract(port.getOwner(), operator);
            }
        }

        private Object extract(SubPlan.Output port) {
            MarkerOperator operator = port.getOperator();
            PlanMarker marker = PlanMarkers.get(operator);
            assert marker != null;
            switch (marker) {
            case CHECKPOINT:
                return operator.getDataType();
            case GATHER:
            case BROADCAST:
                assert operator.getAttribute(EdgeInfo.class) != null;
                return operator.getAttribute(EdgeInfo.class);
            default:
                return PlanAssembler.DEFAULT_EQUIVALENCE.extract(port.getOwner(), operator);
            }
        }
    }

    private static class EdgeInfo {

        private final TypeDescription type;

        private final Group partition;

        private final Object aggregation;

        public EdgeInfo(TypeDescription type, Group partition, Operator aggregation) {
            assert type != null;
            this.type = type;
            this.partition = partition;
            this.aggregation = aggregation == null ? null : aggregation.getOriginalSerialNumber();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + type.hashCode();
            result = prime * result + ((partition == null) ? 0 : partition.hashCode());
            result = prime * result + ((aggregation == null) ? 0 : aggregation.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            EdgeInfo other = (EdgeInfo) obj;
            if (!type.equals(other.type)) {
                return false;
            }
            if (partition == null) {
                if (other.partition != null) {
                    return false;
                }
            } else if (!partition.equals(other.partition)) {
                return false;
            }
            if (aggregation == null) {
                if (other.aggregation != null) {
                    return false;
                }
            } else if (!aggregation.equals(other.aggregation)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "EdgeInfo(group={0}, partial={1})", //$NON-NLS-1$
                    partition,
                    aggregation != null);
        }
    }
}
