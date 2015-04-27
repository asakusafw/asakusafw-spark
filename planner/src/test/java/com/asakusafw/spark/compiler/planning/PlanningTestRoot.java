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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.spark.compiler.planning.PlanningContext.Option;

/**
 * A common test base class for planning.
 */
public abstract class PlanningTestRoot {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * options for testing.
     */
    protected final Set<Option> planningOptions = new LinkedHashSet<>(SparkPlanning.DEFAULT_OPTIONS);

    /**
     * Returns a set of values.
     * @param <T> value type
     * @param values the elements
     * @return the set
     */
    @SafeVarargs
    public static <T> Set<T> set(T... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    /**
     * Returns a matcher whether the operator graph just has the specified operators.
     * @param id operator ID
     * @return the matcher
     */
    public static Matcher<? super Operator> isOperator(String id) {
        return new FeatureMatcher<Operator, String>(equalTo(id), "is operator", "operator") {
            @Override
            protected String featureValueOf(Operator actual) {
                return MockOperators.getId(actual);
            }
        };
    }

    /**
     * Returns a matcher whether the operator graph just has the specified operators.
     * @param ids operator IDs
     * @return the matcher
     */
    public static Matcher<Collection<? extends Operator>> hasOperators(String... ids) {
        return new FeatureMatcher<Collection<? extends Operator>, Set<String>>(
                equalTo(set(ids)), "has operators", "operators") {
            @Override
            protected Set<String> featureValueOf(Collection<? extends Operator> actual) {
                MockOperators mock = new MockOperators(actual);
                Set<String> results = new HashSet<>();
                for (Operator operator : mock.all()) {
                    results.add(mock.id(operator));
                }
                return results;
            }
        };
    }

    /**
     * Returns the unique owner sub-plan from the source operator.
     * @param detail the plan detail
     * @param source the source operator
     * @return the unique sub-plan which is owner of a copy of the target source
     */
    public static SubPlan ownerOf(PlanDetail detail, Operator source) {
        Set<SubPlan> candidates = ownersOf(detail, Collections.singleton(source));
        assertThat(candidates, hasSize(1));
        return candidates.iterator().next();
    }

    /**
     * Returns the unique owner sub-plan from the source operator.
     * @param detail the plan detail
     * @param sources the source operators
     * @return the unique sub-plan which is owner of a copy of the target source
     */
    public static Set<SubPlan> ownersOf(PlanDetail detail, Collection<? extends Operator> sources) {
        Set<Operator> copies = new HashSet<>();
        for (Operator source : sources) {
            copies.addAll(detail.getCopies(source));
        }
        assertThat(copies, is(not(empty())));
        Set<SubPlan> results = new HashSet<>();
        for (Operator copy : copies) {
            results.add(detail.getOwner(copy));
        }
        return results;
    }

    /**
     * Returns the unique owner sub-plan from the source operator.
     * @param detail the plan detail
     * @param inputs the source input operators
     * @param outputs the source outputs operators
     * @return the unique sub-plan which is owner of a copy of the target source
     */
    public static SubPlan ownerOf(
            PlanDetail detail,
            Collection<? extends Operator> inputs,
            Collection<? extends Operator> outputs) {
        Set<Operator> copyInputs = allCopiesOf(detail, inputs);
        Set<Operator> copyOutputs = allCopiesOf(detail, outputs);
        Set<SubPlan> candidates = new HashSet<>();
        for (Operator copy : copyInputs) {
            SubPlan owner = detail.getOwner(copy);
            if (copyInputs.containsAll(toOperators(owner.getInputs()))
                    && copyOutputs.containsAll(toOperators(owner.getOutputs()))) {
                candidates.add(owner);
            }
        }
        assertThat(candidates, hasSize(1));
        return candidates.iterator().next();
    }

    private static Set<Operator> allCopiesOf(PlanDetail detail, Collection<? extends Operator> sources) {
        Set<Operator> results = new HashSet<>();
        for (Operator source : sources) {
            results.addAll(detail.getCopies(source));
        }
        return results;
    }

    /**
     * Returns the operators in the sub-plan ports.
     * @param ports the sub-plan ports
     * @return the operators
     */
    public static Set<MarkerOperator> toOperators(Collection<? extends SubPlan.Port> ports) {
        Set<MarkerOperator> results = new LinkedHashSet<>();
        for (SubPlan.Port port : ports) {
            results.add(port.getOperator());
        }
        return results;
    }

    /**
     * Returns the successors of the sub-plan.
     * @param vertex the sub-plan
     * @return the successors
     */
    public static Set<SubPlan> succ(SubPlan vertex) {
        Set<SubPlan> results = new HashSet<>();
        for (SubPlan.Port port : vertex.getOutputs()) {
            for (SubPlan.Port opposite : port.getOpposites()) {
                results.add(opposite.getOwner());
            }
        }
        return results;
    }

    /**
     * Returns the predecessors of the sub-plan.
     * @param vertex the sub-plan
     * @return the predecessors
     */
    public static Set<SubPlan> pred(SubPlan vertex) {
        Set<SubPlan> results = new HashSet<>();
        for (SubPlan.Port port : vertex.getInputs()) {
            for (SubPlan.Port opposite : port.getOpposites()) {
                results.add(opposite.getOwner());
            }
        }
        return results;
    }

    /**
     * Creates an {@link PlanningContext}.
     * @param keyValuePairs the compiler option properties
     * @return the context
     */
    public PlanningContext context(String... keyValuePairs) {
        assertThat(keyValuePairs.length % 2, is(0));
        CompilerOptions.Builder builder = CompilerOptions.builder();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            builder.withProperty(keyValuePairs[i + 0], keyValuePairs[i + 1]);
        }
        JobflowProcessor.Context context = new MockJobflowProcessorContext(
                builder.build(),
                getClass().getClassLoader(),
                temporary.getRoot());
        return SparkPlanning.createContext(
                context,
                new JobflowInfo.Basic("testing", new ClassDescription("testing")),
                planningOptions);
    }
}