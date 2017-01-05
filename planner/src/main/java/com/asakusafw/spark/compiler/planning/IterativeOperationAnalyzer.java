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
import java.util.Objects;

import com.asakusafw.lang.compiler.common.AttributeContainer;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.Planning;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.spark.compiler.planning.IterativeInfo.RecomputeKind;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * Analyzes iterative operations.
 * @since 0.3.0
 */
public final class IterativeOperationAnalyzer {

    private IterativeOperationAnalyzer() {
        return;
    }

    /**
     * Attaches {@link IterativeInfo} to each element.
     * @param plan the target plan
     */
    public static void attach(Plan plan) {
        Objects.requireNonNull(plan);
        new IterativeOperationAnalyzer().doAttach(plan);
    }

    private void doAttach(Plan target) {
        boolean iterative = false;
        for (SubPlan element : target.getElements()) {
            iterative = isIterative(element);
            if (iterative) {
                break;
            }
        }
        IterativeInfo info = IterativeInfo.never();
        if (iterative) {
            Graph<SubPlan> graph = Planning.toDependencyGraph(target);
            for (SubPlan element : Graphs.sortPostOrder(graph)) {
                doAttach(element);
            }
            info = merge(info, target.getElements());
        }
        target.putAttribute(IterativeInfo.class, info);
    }

    private boolean isIterative(SubPlan element) {
        for (Operator operator : element.getOperators()) {
            IterativeInfo info = IterativeInfo.getDeclared(operator);
            if (info.isIterative()) {
                return true;
            }
        }
        return false;
    }

    private void doAttach(SubPlan target) {
        for (SubPlan.Input port : target.getInputs()) {
            doAttach(port);
        }
        for (SubPlan.Output port : target.getOutputs()) {
            doAttach(port);
        }
        IterativeInfo info = IterativeInfo.never();
        info = merge(info, target.getInputs());
        info = merge(info, target.getOutputs());
        target.putAttribute(IterativeInfo.class, info);
    }

    private void doAttach(SubPlan.Input target) {
        IterativeInfo info = IterativeInfo.never();
        info = merge(info, target.getOpposites());
        target.putAttribute(IterativeInfo.class, info);
    }

    private void doAttach(SubPlan.Output target) {
        SubPlan owner = target.getOwner();
        IterativeInfo info = IterativeInfo.never();
        for (Operator upstream : Operators.getTransitivePredecessors(target.getOperator().getInputs())) {
            SubPlan.Input source = owner.findInput(upstream);
            if (source != null) {
                info = info.merge(IterativeInfo.get(source));
            } else if (upstream.getOperatorKind() == OperatorKind.OUTPUT) {
                // external output always requires re-computing
                info = IterativeInfo.always();
            } else {
                info = info.merge(IterativeInfo.getDeclared(upstream));
            }
            if (info.getRecomputeKind() == RecomputeKind.ALWAYS) {
                break;
            }
        }
        target.putAttribute(IterativeInfo.class, info);
    }

    private IterativeInfo merge(IterativeInfo start, Collection<? extends AttributeContainer> elements) {
        IterativeInfo current = start;
        for (AttributeContainer element : elements) {
            current = current.merge(IterativeInfo.getInfo(element));
        }
        return current;
    }
}
