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

import java.util.HashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OptimizerContext;
import com.asakusafw.lang.compiler.optimizer.adapter.OperatorEstimatorAdapter;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Estimates the size of data-sets on sub-plan inputs/outputs.
 */
@SuppressWarnings("unused")
public class PlanEstimator {

    private final OperatorEstimator estimator;

    private final OperatorEstimator.Context context;

    private final Set<SubPlan> performed = new HashSet<>();

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
     * @param port the targer port
     * @return the size infomation
     */
    public SizeInfo estimate(SubPlan.Port port) {
        // FIXME implement
        return SizeInfo.UNKNOWN;
    }
}
