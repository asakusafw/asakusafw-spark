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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.annotation.Annotation;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.spark.compiler.planning.PartitionGroupInfo.DataSize;
import com.asakusafw.vocabulary.operator.CoGroup;
import com.asakusafw.vocabulary.operator.Extract;

/**
 * Test for {@link PartitionGroupAnalyzer}.
 */
public class PartitionGroupAnalyzerTest extends PlanningTestRoot {

    /**
     * load limit map - simple case.
     */
    @Test
    public void load_limits_simple() {
        CompilerOptions options = CompilerOptions.builder()
            .withProperty(limits(DataSize.SMALL), String.valueOf(10L * 1024 * 1024))
            .build();
        Map<DataSize, Double> map = PartitionGroupAnalyzer.loadLimitMap(options);

        assertThat(map, hasKey(DataSize.TINY));
        assertThat(map, hasEntry(DataSize.SMALL, 10.0 * 1024 * 1024));
    }

    /**
     * load limit map - simple case.
     */
    @Test
    public void load_limits_all() {
        CompilerOptions.Builder builder = CompilerOptions.builder();
        for (DataSize size : DataSize.values()) {
            builder.withProperty(limits(size), String.valueOf(size.ordinal()));
        }
        Map<DataSize, Double> map = PartitionGroupAnalyzer.loadLimitMap(builder.build());
        for (DataSize size : DataSize.values()) {
            assertThat(map, hasEntry(size, (double) size.ordinal()));
        }
    }

    /**
     * load limit map - simple case.
     */
    @Test
    public void load_limits_invalid() {
        CompilerOptions.Builder builder = CompilerOptions.builder();
        for (DataSize size : DataSize.values()) {
            builder.withProperty(limits(size), String.valueOf("invalid"));
        }
        Map<DataSize, Double> map = PartitionGroupAnalyzer.loadLimitMap(builder.build());
        assertThat(map, is(PartitionGroupAnalyzer.DEFAULT_LIMITS));
    }

    /**
     * load explicit size map - simple case.
     */
    @Test
    public void load_explicits_simple() {
        MockOperators m = new MockOperators();
        PlanningContext context = context(explicit("a"), "small");
        PlanDetail detail = SparkPlanning.plan(context, m
            .input("in")
            .bless("o0", op(CoGroup.class, "a")
                    .input("in", m.getCommonDataType(), group("=a"))
                    .output("out", m.getCommonDataType()))
                .connect("in", "o0")
            .output("out").connect("o0", "out")
            .toGraph());
        MockOperators mock = restore(detail);

        Plan plan = detail.getPlan();
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        CompilerOptions options = context.getOptimizerContext().getOptions();
        Map<SubPlan, DataSize> explicits = PartitionGroupAnalyzer.loadExplicitSizeMap(options, plan);
        assertThat(explicits.keySet(), hasSize(1));
        assertThat(explicits, hasEntry(s1, DataSize.SMALL));
    }

    /**
     * load explicit size map - merge sizes.
     */
    @Test
    public void load_explicits_merge() {
        MockOperators m = new MockOperators();
        PlanningContext context = context(
                explicit("a"), "small",
                explicit("b"), "large");
        PlanDetail detail = SparkPlanning.plan(context, m
            .input("in")
            .bless("o0", op(CoGroup.class, "a")
                    .input("in", m.getCommonDataType(), group("=a"))
                    .output("out", m.getCommonDataType()))
                .connect("in", "o0")
            .operator(op(Extract.class, "b"), "o1").connect("o0", "o1")
            .output("out").connect("o1", "out")
            .toGraph());
        MockOperators mock = restore(detail);

        Plan plan = detail.getPlan();
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        CompilerOptions options = context.getOptimizerContext().getOptions();
        Map<SubPlan, DataSize> explicits = PartitionGroupAnalyzer.loadExplicitSizeMap(options, plan);
        assertThat(explicits.keySet(), hasSize(1));
        assertThat(explicits, hasEntry(s1, DataSize.LARGE));
    }

    private String limits(DataSize size) {
        return PartitionGroupAnalyzer.KEY_LIMIT_PREFIX + size.getSymbol();
    }

    private String explicit(String name) {
        return String.format(
                "%s%s.%s",
                PartitionGroupAnalyzer.KEY_OPERATOR_PREFIX,
                Ops.class.getName(),
                name);
    }

    private static UserOperator.Builder op(Class<? extends Annotation> annotation, String name) {
        return OperatorExtractor.extract(annotation, Ops.class, name);
    }

    @SuppressWarnings("javadoc")
    public abstract static class Ops {

        @CoGroup
        public abstract void a();

        @Extract
        public abstract void b();
    }
}
