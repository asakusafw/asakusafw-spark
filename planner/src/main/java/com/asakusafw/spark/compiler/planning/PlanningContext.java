/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
import java.util.Set;

import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizer;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.lang.compiler.optimizer.OptimizerContext;
import com.asakusafw.lang.compiler.optimizer.basic.BasicOptimizers;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass;
import com.asakusafw.lang.compiler.planning.util.GraphStatistics;

/**
 * Context object for execution planner.
 */
public class PlanningContext {

    private final OptimizerContext optimizerContext;

    private final OperatorEstimator estimator;

    private final OperatorCharacterizer<? extends OperatorClass> classifier;

    private final OperatorRewriter rewriter;

    private final Set<Option> options;

    /**
     * Creates a new instance.
     * @param context the parent optimizer context
     * @param options the planning options
     */
    public PlanningContext(OptimizerContext context, Collection<Option> options) {
        this(context,
                BasicOptimizers.getDefaultEstimator(context.getClassLoader()).build(),
                BasicOptimizers.getDefaultClassifier(context.getClassLoader()).build(),
                BasicOptimizers.getDefaultRewriter(context.getClassLoader()).build(),
                options);
    }

    /**
     * Creates a new instance.
     * @param context the parent optimizer context
     * @param estimator the operator estimator
     * @param classifier the operator classifier
     * @param rewriter the operator graph rewriter
     * @param options the planning options
     */
    public PlanningContext(
            OptimizerContext context,
            OperatorEstimator estimator,
            OperatorCharacterizer<? extends OperatorClass> classifier,
            OperatorRewriter rewriter,
            Collection<Option> options) {
        this.optimizerContext = context;
        this.estimator = estimator;
        this.classifier = classifier;
        this.rewriter = rewriter;
        this.options = Collections.unmodifiableSet(new HashSet<>(options));
    }

    /**
     * Returns the current optimizer context.
     * @return the optimizer context
     */
    public OptimizerContext getOptimizerContext() {
        return optimizerContext;
    }

    /**
     * Returns the operator estimator.
     * @return the operator estimator
     */
    public OperatorEstimator getEstimator() {
        return estimator;
    }

    /**
     * Returns the operator classifier.
     * @return the operator classifier
     */
    public OperatorCharacterizer<? extends OperatorClass> getClassifier() {
        return classifier;
    }

    /**
     * Returns the operator graph rewriter.
     * @return the operator graph rewriter
     */
    public OperatorRewriter getRewriter() {
        return rewriter;
    }

    /**
     * Returns the planning options.
     * @return the options
     */
    public Set<Option> getOptions() {
        return options;
    }

    /**
     * Represents an option for planning.
     * @since 0.1.0
     * @version 0.5.2
     */
    public enum Option {

        /**
         * Enables to unify sub-plan I/O ports.
         */
        UNIFY_SUBPLAN_IO(true),

        /**
         * Inserts {@code CHECKPOINT} after external inputs.
         */
        CHECKPOINT_AFTER_EXTERNAL_INPUTS(false),

        /**
         * Inserts {@code CHECKPOINT} before external outputs.
         */
        CHECKPOINT_BEFORE_EXTERNAL_OUTPUTS(true),

        /**
         * Removes cyclic broadcast operations.
         * @since 0.5.2
         */
        REMOVE_CYCLIC_BROADCASTS(true),

        /**
         * Enables {@link SizeInfo} and {@link PartitionGroupInfo} for sub-plan I/O ports.
         */
        SIZE_ESTIMATION(true),

        /**
         * Enables {@link GraphStatistics}.
         */
        GRAPH_STATISTICS(true),

        /**
         * Enables {@link PlanStatistics}.
         */
        PLAN_STATISTICS(true),

        /**
         * Enables {@link IterativeInfo}.
         * @since 0.3.0
         */
        ITERATIVE_EXTENSION(true),

        /**
         * Enables {@link ParameterInfo}.
         * @since 0.3.0
         */
        PARAMETER_DETECTION(true),
        ;

        private String symbol;

        private boolean defaultEnabled;

        Option(boolean defaultEnabled) {
            this.symbol = toSymbol(name());
            this.defaultEnabled = defaultEnabled;
        }

        private static String toSymbol(String name) {
            boolean upper = false;
            StringBuilder buf = new StringBuilder();
            for (char c : name.toCharArray()) {
                if (c == '_') {
                    upper = true;
                } else if (upper) {
                    buf.append(Character.toUpperCase(c));
                    upper = false;
                } else {
                    buf.append(Character.toLowerCase(c));
                }
            }
            return buf.toString();
        }

        /**
         * Returns the symbol of this option.
         * @return the symbol
         */
        public String getSymbol() {
            return symbol;
        }

        /**
         * Returns whether this option is enabled in default or not.
         * @return {@code true} if this option is enabled in default, otherwise {@code false}
         */
        public boolean isDefaultEnabled() {
            return defaultEnabled;
        }
    }
}
