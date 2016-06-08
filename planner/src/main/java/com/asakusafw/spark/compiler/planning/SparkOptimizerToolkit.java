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

import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorPort;
import com.asakusafw.lang.compiler.optimizer.OptimizerToolkit;
import com.asakusafw.lang.compiler.optimizer.basic.BasicOptimizerToolkit;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.Planning;

/**
 * An {@link OptimizerToolkit} for Asakusa DAG compiler.
 * @since 0.3.1
 */
public final class SparkOptimizerToolkit extends BasicOptimizerToolkit {

    /**
     * The singleton instance.
     */
    public static final SparkOptimizerToolkit INSTANCE = new SparkOptimizerToolkit();

    private SparkOptimizerToolkit() {
        return;
    }

    @Override
    public void repair(OperatorGraph graph) {
        Planning.normalize(graph);
    }

    @Override
    public boolean hasEffectiveOpposites(OperatorInput port) {
        return hasEffectiveOpposites(port, PlanMarker.BEGIN);
    }

    @Override
    public boolean hasEffectiveOpposites(OperatorOutput port) {
        return hasEffectiveOpposites(port, PlanMarker.END);
    }

    private boolean hasEffectiveOpposites(OperatorPort port, PlanMarker ignore) {
        for (OperatorPort opposite : port.getOpposites()) {
            if (PlanMarkers.get(opposite.getOwner()) == ignore) {
                continue;
            }
            return true;
        }
        return false;
    }
}
