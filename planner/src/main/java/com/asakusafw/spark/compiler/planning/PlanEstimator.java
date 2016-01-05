/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimate;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OptimizerContext;
import com.asakusafw.lang.compiler.optimizer.adapter.OperatorEstimatorAdapter;
import com.asakusafw.lang.compiler.optimizer.basic.BasicOperatorEstimate;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * Estimates the size of data-sets on sub-plan inputs/outputs.
 */
public class PlanEstimator {

    private final OperatorEstimator estimator;

    private final OperatorEstimatorAdapter context;

    private final Set<SubPlan> resolved = new HashSet<>();

    /**
     * Creates a new instance.
     * @param estimator the operator size estimator
     * @param context the current optimizer context
     */
    public PlanEstimator(OperatorEstimator estimator, OptimizerContext context) {
        this.estimator = estimator;
        this.context = new OperatorEstimatorAdapter(context);
    }

    /**
     * Estimates the size of port.
     * @param port the target port
     * @return the size information
     */
    public SizeInfo estimate(SubPlan.Port port) {
        SubPlan owner = port.getOwner();
        prepare(owner);
        assert resolved.contains(owner);
        OperatorEstimate estimate = context.estimate(port.getOperator());
        return new SizeInfo(estimate.getSize(port.getOperator().getInput()));
    }

    private void prepare(SubPlan sub) {
        if (resolved.contains(sub)) {
            return;
        }
        List<SubPlan> list = sortUnresolved(Collections.singleton(sub));
        for (SubPlan element : list) {
            assert resolved.contains(element) == false;
            prepareFlat(element);
        }
    }

    private List<SubPlan> sortUnresolved(Collection<? extends SubPlan> subs) {
        Graph<SubPlan> graph = Graphs.newInstance();
        LinkedList<SubPlan> work = new LinkedList<>(subs);
        while (work.isEmpty() == false) {
            SubPlan first = work.removeFirst();
            if (resolved.contains(first)) {
                continue;
            }
            graph.addNode(first);
            for (SubPlan.Input input : first.getInputs()) {
                for (SubPlan.Output opposite : input.getOpposites()) {
                    SubPlan upstream = opposite.getOwner();
                    if (resolved.contains(upstream)) {
                        continue;
                    }
                    graph.addEdge(first, upstream);
                    work.add(upstream);
                }
            }
        }
        return Graphs.sortPostOrder(graph);
    }

    private void prepareFlat(SubPlan sub) {
        for (SubPlan.Input port : sub.getInputs()) {
            propagateInputs(port);
        }
        Set<Operator> outputs = new HashSet<>();
        for (SubPlan.Output port : sub.getOutputs()) {
            outputs.add(port.getOperator());
        }
        context.apply(estimator, outputs);
        resolved.add(sub);
    }

    private void propagateInputs(SubPlan.Input port) {
        Set<MarkerOperator> upstreams = new HashSet<>();
        for (SubPlan.Output opposite : port.getOpposites()) {
            upstreams.add(opposite.getOperator());
        }
        Map<Operator, OperatorEstimate> estimates = context.estimate(upstreams);
        double total = 0.0;
        for (Map.Entry<Operator, OperatorEstimate> entry : estimates.entrySet()) {
            MarkerOperator operator = (MarkerOperator) entry.getKey();
            OperatorEstimate estimate = entry.getValue();
            double fragment = estimate.getSize(operator.getOutput());
            if (Double.isNaN(fragment)) {
                total = Double.NaN;
                break;
            } else {
                total += fragment;
            }
        }

        BasicOperatorEstimate results = new BasicOperatorEstimate();
        results.putSize(port.getOperator().getInput(), total);
        results.putSize(port.getOperator().getOutput(), total);
        context.put(port.getOperator(), results);
    }
}
