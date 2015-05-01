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

import java.lang.annotation.Annotation;
import java.util.Set;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo.DataSize;
import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.spark.compiler.planning.SubPlanInfo.DriverOption;
import com.asakusafw.spark.compiler.planning.SubPlanInfo.DriverType;
import com.asakusafw.spark.compiler.planning.SubPlanInputInfo.InputOption;
import com.asakusafw.spark.compiler.planning.SubPlanInputInfo.InputType;
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo.OutputOption;
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo.OutputType;
import com.asakusafw.vocabulary.flow.processor.PartialAggregation;
import com.asakusafw.vocabulary.operator.CoGroup;
import com.asakusafw.vocabulary.operator.Extract;
import com.asakusafw.vocabulary.operator.Fold;
import com.asakusafw.vocabulary.operator.MasterJoinUpdate;

/**
 * Test for {@link SparkPlanning}.
 */
public class SparkPlanningTest extends PlanningTestRoot {

    /**
     * simple case.
<pre>{@code
in --- out
==>
in --- *C --- out
}</pre>
     */
    @Test
    public void simple() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
            .input("in")
            .output("out").connect("in", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(2));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("out"));

        assertThat(info(s0).toString(), s0, primaryOperator(isOperator("in")));
        assertThat(s0, driverType(is(DriverType.INPUT)));
        assertThat(s0, not(driverOption(is(DriverOption.PARTIAL))));
        assertThat(input(s0), inputType(is(InputType.VOID)));
        assertThat(input(s0), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s0), inputPartition(is(nullValue())));
        assertThat(output(s0), outputType(is(OutputType.DONT_CARE)));
        assertThat(output(s0), outputPartition(is(nullValue())));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(info(s1).toString(), s1, primaryOperator(isOperator("out")));
        assertThat(s1, driverType(is(DriverType.OUTPUT)));
        assertThat(s1, not(driverOption(is(DriverOption.PARTIAL))));
        assertThat(input(s1), inputType(is(InputType.DONT_CARE)));
        assertThat(input(s1), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s1), inputPartition(is(nullValue())));
        assertThat(output(s1), outputType(is(OutputType.DISCARD)));
        assertThat(output(s1), outputPartition(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
    }

    /**
     * extract kind.
<pre>{@code
in --- c0 --- o0 --- out
==>
in --- *C --- o0 --- *C --- out
}</pre>
     */
    @Test
    public void extract_kind() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
            .input("in")
            .operator(cp(), "c0").connect("in", "c0")
            .operator(op(Extract.class, "extract"), "o0").connect("c0", "o0")
            .output("out").connect("o0", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        assertThat("checkpoint operator should be replaced", mock.all(), not(hasItem(isOperator("c0"))));

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        assertThat(output(s0), outputType(is(OutputType.DONT_CARE)));
        assertThat(output(s0), outputPartition(is(nullValue())));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(info(s1).toString(), s1, primaryOperator(nullValue()));
        assertThat(s1, driverType(is(DriverType.EXTRACT)));
        assertThat(s1, not(driverOption(is(DriverOption.PARTIAL))));
        assertThat(input(s1), inputType(is(InputType.DONT_CARE)));
        assertThat(input(s1), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s1), inputPartition(is(nullValue())));
        assertThat(output(s1), outputType(is(OutputType.DONT_CARE)));
        assertThat(output(s1), outputPartition(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
    }

    /**
     * co-group kind.
<pre>{@code
in --- o0 --- out
==>
in --- *G --- o0 --- *C --- out
}</pre>
     */
    @Test
    public void cogroup_kind() {
        MockOperators m = new MockOperators();
        PlanDetail detail = SparkPlanning.plan(context(), m
            .input("in")
            .bless("o0", op(CoGroup.class, "cogroup")
                    .input("in", m.getCommonDataType(), group("=a"))
                    .output("out", m.getCommonDataType()))
                .connect("in", "o0")
            .output("out").connect("o0", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        assertThat(output(s0), outputType(is(OutputType.PARTITIONED)));
        assertThat(output(s0), outputPartition(is(group("=a"))));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(info(s1).toString(), s1, primaryOperator(isOperator("o0")));
        assertThat(s1, driverType(is(DriverType.COGROUP)));
        assertThat(s1, not(driverOption(is(DriverOption.PARTIAL))));
        assertThat(input(s1), inputType(is(InputType.PARTITIONED)));
        assertThat(input(s1), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s1), inputPartition(is(group("=a"))));
        assertThat(output(s1), outputType(is(OutputType.DONT_CARE)));
        assertThat(output(s1), outputPartition(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
    }

    /**
     * aggregate (total) kind.
<pre>{@code
in ---  o0 --- out
==>
in --- *G --- o0 --- *C --- out
}</pre>
     */
    @Test
    public void aggregate_kind_total() {
        MockOperators m = new MockOperators();
        PlanDetail detail = SparkPlanning.plan(context(), m
            .input("in")
            .bless("o0", op(Fold.class, "fold_total")
                    .input("in", m.getCommonDataType(), group("=a"))
                    .output("out", m.getCommonDataType()))
                    .connect("in", "o0")
            .output("out").connect("o0", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        assertThat(output(s0), outputType(is(OutputType.PARTITIONED)));
        assertThat(output(s0), outputPartition(is(group("=a"))));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(info(s1).toString(), s1, primaryOperator(isOperator("o0")));
        assertThat(s1, driverType(is(DriverType.AGGREGATE)));
        assertThat(s1, not(driverOption(is(DriverOption.PARTIAL))));
        assertThat(input(s1), inputType(is(InputType.PARTITIONED)));
        assertThat(input(s1), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s1), inputPartition(is(group("=a"))));
        assertThat(output(s1), outputType(is(OutputType.DONT_CARE)));
        assertThat(output(s1), outputPartition(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
    }

    /**
     * aggregate (total) kind.
<pre>{@code
in --- o0 --- out
==>
in --- *G --- o0 --- *C --- out
}</pre>
     */
    @Test
    public void aggregate_kind_partial() {
        MockOperators m = new MockOperators();
        PlanDetail detail = SparkPlanning.plan(context(), m
                .input("in")
                .bless("o0", op(Fold.class, "fold_partial")
                        .input("in", m.getCommonDataType(), group("=a"))
                        .output("out", m.getCommonDataType()))
                        .connect("in", "o0")
                .output("out").connect("o0", "out")
                .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        assertThat(output(s0), outputType(is(OutputType.AGGREGATED)));
        assertThat(output(s0), outputPartition(is(group("=a"))));
        assertThat(output(s0), outputAggregation(isOperator("o0")));

        assertThat(info(s1).toString(), s1, primaryOperator(isOperator("o0")));
        assertThat(s1, driverType(is(DriverType.AGGREGATE)));
        assertThat(s1, driverOption(is(DriverOption.PARTIAL)));
        assertThat(input(s1), inputType(is(InputType.PARTITIONED)));
        assertThat(input(s1), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s1), inputPartition(is(group("=a"))));
        assertThat(output(s1), outputType(is(OutputType.DONT_CARE)));
        assertThat(output(s1), outputPartition(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
    }

    /**
     * with broadcast from different origins.
<pre>{@code
in0 --+ o0 --- out
in1 -/
==>
       in0 --+ o0 --- *C --- out
in1 --- *B -/
}</pre>
     */
    @Test
    public void broadcast() {
        MockOperators m = new MockOperators();
        PlanDetail detail = SparkPlanning.plan(context(), m
            .input("in0", DataSize.LARGE)
            .input("in1", DataSize.TINY)
            .bless("o0", newJoin(m))
                .connect("in0", "o0.t")
                .connect("in1", "o0.m")
            .output("out").connect("o0.f", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));

        assertThat(info(s0).toString(), s0, primaryOperator(isOperator("in0")));
        assertThat(s0, driverType(is(DriverType.INPUT)));
        assertThat(s0, not(driverOption(is(DriverOption.PARTIAL))));

        assertThat(output(s1), outputType(is(OutputType.BROADCAST)));
        assertThat(output(s1), outputPartition(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
        assertThat(output(s1), broadcastFormat(is(group("+k"))));

        assertThat(output(s1).getOpposites(), hasSize(1));
        SubPlan.Input bIn = output(s1).getOpposites().iterator().next();
        assertThat(bIn, inputType(is(InputType.BROADCAST)));
        assertThat(bIn, not(inputOption(is(InputOption.PRIMARY))));
        assertThat(bIn, inputPartition(is(nullValue())));
        assertThat(bIn, broadcastFormat(is(group("+k"))));
    }

    /**
     * with broadcast from same origin.
<pre>{@code
in0 +---+ o0 --- out
     \-/
==>
       in0 --+ o0 --- *C --- out
in0 --- *B -/
}</pre>
     */
    @Test
    public void broadcast_same_origin() {
        MockOperators m = new MockOperators();
        PlanDetail detail = SparkPlanning.plan(context(), m
            .input("in0", DataSize.TINY)
            .bless("o0", newJoin(m))
                .connect("in0", "o0.t")
                .connect("in0", "o0.m")
            .output("out").connect("o0.f", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        SubPlan s1 = pred(s0).iterator().next();

        assertThat(info(s0).toString(), s0, primaryOperator(isOperator("in0")));
        assertThat(s0, driverType(is(DriverType.INPUT)));
        assertThat(s0, not(driverOption(is(DriverOption.PARTIAL))));

        assertThat(output(s1), outputType(is(OutputType.BROADCAST)));
        assertThat(output(s1), outputPartition(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
        assertThat(output(s1), broadcastFormat(is(group("+k"))));

        assertThat(output(s1).getOpposites(), hasSize(1));
        SubPlan.Input bIn = output(s1).getOpposites().iterator().next();
        assertThat(bIn, inputType(is(InputType.BROADCAST)));
        assertThat(bIn, not(inputOption(is(InputOption.PRIMARY))));
        assertThat(bIn, inputPartition(is(nullValue())));
        assertThat(bIn, broadcastFormat(is(group("+k"))));
    }

    /**
     * multiple broadcast inputs.
<pre>{@code
in0 --+ o0 --+ o1 --+ o2 --- out
in1 -/      /      /
in2 -------/      /
in3 -------------/
==>
       in0 --+ o0 --+ o1 --+ o2 --- *C --- out
in1 --- *B -/      /      /
in2 --- *B -------/      /
in3 --- *B -------------/
}</pre>
     */
    @Test
    public void broadcast_chain() {
        MockOperators m = new MockOperators();
        PlanDetail detail = SparkPlanning.plan(context(), m
            .input("in0", DataSize.LARGE)
            .input("in1", DataSize.TINY)
            .input("in2", DataSize.TINY)
            .input("in3", DataSize.TINY)
            .bless("o0", newJoin(m))
                .connect("in0", "o0.t")
                .connect("in1", "o0.m")
            .bless("o1", newJoin(m))
                .connect("o0.f", "o1.t")
                .connect("in2", "o1.m")
            .bless("o2", newJoin(m))
                .connect("o1.f", "o2.t")
                .connect("in3", "o2.m")
            .output("out").connect("o2.f", "out")
            .toGraph());
        MockOperators mock = restore(detail);

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(5));

        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));
        SubPlan s2 = ownerOf(detail, mock.get("in2"));
        SubPlan s3 = ownerOf(detail, mock.get("in3"));
        SubPlan s4 = ownerOf(detail, mock.get("out"));
        assertThat("each sub-plan is identical", set(s0, s1, s2, s3, s4), hasSize(5));

        assertThat(s0.getOperators(), hasOperators("in0", "in0", "o0", "o1", "o2"));
        assertThat(s1.getOperators(), hasOperators("in1"));
        assertThat(s2.getOperators(), hasOperators("in2"));
        assertThat(s3.getOperators(), hasOperators("in3"));
        assertThat(s4.getOperators(), hasOperators("out"));
    }

    /**
     * through kind.
<pre>{@code
in --- c0 --- o0 --- out
==>
in --- *C --- *G --- o0 --- *C --- out
          ~~~ <- through
}</pre>
     */
    @Test
    public void through() {
        MockOperators m = new MockOperators();
        PlanDetail detail = SparkPlanning.plan(context(), m
            .input("in")
            .operator(cp(), "c0").connect("in", "c0")
            .bless("o0", op(CoGroup.class, "cogroup")
                    .input("in", m.getCommonDataType(), group("=a"))
                    .output("out", m.getCommonDataType()))
                    .connect("c0", "o0")
            .output("out").connect("o0", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(4));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = succ(s0).iterator().next();
        SubPlan s2 = ownerOf(detail, mock.get("o0"));
        assertThat(s1, is(not(s2)));

        assertThat(output(s0), outputType(is(OutputType.DONT_CARE)));
        assertThat(output(s0), outputPartition(is(nullValue())));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(info(s1).toString(), s1, driverType(is(DriverType.EXTRACT)));
        assertThat(s1, primaryOperator(nullValue()));
    }

    /**
     * co-group with several inputs are open.
     */
    @Test
    public void cogroup_partial() {
        MockOperators m = new MockOperators();
        PlanDetail detail = SparkPlanning.plan(context(), m
            .input("in")
            .bless("o0", op(CoGroup.class, "cogroup")
                    .input("a", m.getCommonDataType(), group("=k"))
                    .input("b", m.getCommonDataType(), group("=k"))
                    .output("out", m.getCommonDataType()))
                .connect("in", "o0.a")
            .output("out").connect("o0", "out")
            .toGraph());
        MockOperators mock = restore(detail);

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        assertThat(s0.getInputs(), hasSize(1));
        assertThat(output(s0), outputType(is(OutputType.PARTITIONED)));
        assertThat(output(s0), outputPartition(is(group("=k"))));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(s1.getInputs(), hasSize(1));
    }

    /**
     * join with broadcast input is open.
     */
    @Test
    public void broadcast_partial() {
        MockOperators m = new MockOperators();
        PlanDetail detail = SparkPlanning.plan(context(), m
            .input("in0", DataSize.LARGE)
            .bless("o0", newJoin(m))
                .connect("in0", "o0.t")
            .output("out").connect("o0.f", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(2));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        assertThat(s0.getInputs(), hasSize(1));
    }

    /**
     * sub-plan w/ multiple dominant operators.
<pre>{@code
in --- c0 +--- o0 ---+ out
           \-- o1 --/
            \- o2 -/
==>
in --- *C +--- o0 ---+ *C --- out
           \-- o1 --/
            \- o2 -/
}</pre>
     */
    @Test
    public void multiple_dominant_operators() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
            .input("in")
            .operator(cp(), "c0").connect("in", "c0")
            .operator(op(Extract.class, "extract"), "o0").connect("c0", "o0")
            .operator(op(Extract.class, "extract"), "o1").connect("c0", "o1")
            .operator(op(Extract.class, "extract"), "o2").connect("c0", "o2")
            .output("out")
                .connect("o0", "out")
                .connect("o1", "out")
                .connect("o2", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));
        SubPlan s2 = ownerOf(detail, mock.get("out"));

        assertThat(info(s0).toString(), s0.getOperators(), hasOperators("in"));
        assertThat(info(s1).toString(), s1.getOperators(), hasOperators("o0", "o1", "o2"));
        assertThat(info(s2).toString(), s2.getOperators(), hasOperators("out"));
    }

    /**
     * each sub-plans should be unified with a focus on the inputs, and they are individual.
     */
    @Test
    public void unify_focus_input() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
            .input("in0")
            .input("in1")
            .input("in2")
            .operator(op(Extract.class, "extract"), "o0")
                .connect("in0", "o0")
                .connect("in1", "o0")
                .connect("in2", "o0")
            .output("out0").connect("o0", "out0")
            .output("out1").connect("o0", "out1")
            .output("out2").connect("o0", "out2")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(6));

        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));
        SubPlan s2 = ownerOf(detail, mock.get("in2"));
        SubPlan s3 = ownerOf(detail, mock.get("out0"));
        SubPlan s4 = ownerOf(detail, mock.get("out1"));
        SubPlan s5 = ownerOf(detail, mock.get("out2"));
        assertThat("each sub-plan is identical", set(s0, s1, s2, s3, s4, s5), hasSize(6));

        assertThat(s0.getOperators(), hasOperators("in0", "o0"));
        assertThat(s1.getOperators(), hasOperators("in1", "o0"));
        assertThat(s2.getOperators(), hasOperators("in2", "o0"));
        assertThat(s3.getOperators(), hasOperators("out0"));
        assertThat(s4.getOperators(), hasOperators("out1"));
        assertThat(s5.getOperators(), hasOperators("out2"));
    }

    /**
     * each sub-plans should be unified with a focus on the inputs, and they are individual.
     */
    @Test
    public void unify_focus_input_complex() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
            .input("in0")
            .input("in1")
            .input("in2")
            .operator(op(Extract.class, "extract"), "o0")
                .connect("in0", "o0")
                .connect("in1", "o0")
                .connect("in2", "o0")
            .operator(op(Extract.class, "extract"), "o1")
                .connect("in0", "o1")
                .connect("in1", "o1")
                .connect("in2", "o1")
            .operator(op(Extract.class, "extract"), "o2")
                .connect("in0", "o2")
                .connect("in1", "o2")
                .connect("in2", "o2")
            .output("out0")
                .connect("o0", "out0")
                .connect("o1", "out0")
                .connect("o2", "out0")
            .output("out1")
                .connect("o0", "out1")
                .connect("o1", "out1")
                .connect("o2", "out1")
            .output("out2")
                .connect("o0", "out2")
                .connect("o1", "out2")
                .connect("o2", "out2")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(6));

        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));
        SubPlan s2 = ownerOf(detail, mock.get("in2"));
        SubPlan s3 = ownerOf(detail, mock.get("out0"));
        SubPlan s4 = ownerOf(detail, mock.get("out1"));
        SubPlan s5 = ownerOf(detail, mock.get("out2"));
        assertThat("each sub-plan is identical", set(s0, s1, s2, s3, s4, s5), hasSize(6));

        assertThat(s0.getOperators(), hasOperators("in0", "o0", "o1", "o2"));
        assertThat(s1.getOperators(), hasOperators("in1", "o0", "o1", "o2"));
        assertThat(s2.getOperators(), hasOperators("in2", "o0", "o1", "o2"));
        assertThat(s3.getOperators(), hasOperators("out0"));
        assertThat(s4.getOperators(), hasOperators("out1"));
        assertThat(s5.getOperators(), hasOperators("out2"));
    }

    /**
     * equivalent sub-plan outputs (checkpoint) should be unified.
<pre>{@code
in --- o0 +--- out0
           \-- out1
            \- out2
}</pre>
     */
    @Test
    public void unify_subplan_output_checkpoint() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
            .input("in")
            .operator(op(Extract.class, "extract"), "o0").connect("in", "o0")
            .output("out0").connect("o0", "out0")
            .output("out1").connect("o0", "out1")
            .output("out2").connect("o0", "out2")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(4));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        assertThat(s0.getOutputs(), hasSize(1));
    }

    /**
     * equivalent sub-plan outputs (broadcast) should be unified.
<pre>{@code
in0 --+ o0 --+ o1 --+ o2 --- out
in1 -/------/------/
}</pre>
     */
    @Test
    public void unify_subplan_output_broadcast() {
        MockOperators m = new MockOperators();
        PlanDetail detail = SparkPlanning.plan(context(), m
            .input("in0", DataSize.LARGE)
            .input("in1", DataSize.TINY)
            .bless("o0", newJoin(m))
                .connect("in0", "o0.t")
                .connect("in1", "o0.m")
            .bless("o1", newJoin(m))
                .connect("o0.f", "o1.t")
                .connect("in1", "o1.m")
            .bless("o2", newJoin(m))
                .connect("o1.f", "o2.t")
                .connect("in1", "o2.m")
            .output("out").connect("o2.f", "out")
            .toGraph());
        MockOperators mock = restore(detail);

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));
        assertThat(s0.getInputs(), hasSize(2));
        assertThat(s1.getOutputs(), hasSize(1));
    }

    /**
     * equivalent sub-plan outputs (gather) should be unified.
     */
    @Test
    public void unify_subplan_output_gather() {
        MockOperators m = new MockOperators();
        PlanDetail detail = SparkPlanning.plan(context(), m
            .input("in0")
            .input("in1")
            .bless("o0", op(CoGroup.class, "cogroup")
                    .input("a", m.getCommonDataType(), group("=k0", "+k1"))
                    .input("b", m.getCommonDataType(), group("=k0", "-k1"))
                    .output("out", m.getCommonDataType()))
                .connect("in0", "o0.a")
                .connect("in1", "o0.b")
            .bless("o1", op(CoGroup.class, "cogroup")
                    .input("a", m.getCommonDataType(), group("=k0", "+k1"))
                    .input("b", m.getCommonDataType(), group("=k0", "-k1"))
                    .output("out", m.getCommonDataType()))
                .connect("in0", "o1.a")
                .connect("in1", "o1.b")
            .output("out")
                .connect("o0", "out")
                .connect("o1", "out")
            .toGraph());
        MockOperators mock = restore(detail);

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(5));

        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));
        SubPlan s2 = ownerOf(detail, mock.get("o0"));
        SubPlan s3 = ownerOf(detail, mock.get("o1"));

        assertThat(s0.getOutputs(), hasSize(1));
        assertThat(s1.getOutputs(), hasSize(1));
        assertThat(s2.getInputs(), hasSize(2));
        assertThat(s3.getInputs(), hasSize(2));
    }

    static SubPlanInfo info(SubPlan container) {
        SubPlanInfo info = container.getAttribute(SubPlanInfo.class);
        assertThat(String.valueOf(info), info, is(notNullValue()));
        return info;
    }

    static SubPlanInputInfo info(SubPlan.Input container) {
        SubPlanInputInfo info = container.getAttribute(SubPlanInputInfo.class);
        assertThat(String.valueOf(info), info, is(notNullValue()));
        return info;
    }

    static SubPlanOutputInfo info(SubPlan.Output container) {
        SubPlanOutputInfo info = container.getAttribute(SubPlanOutputInfo.class);
        assertThat(String.valueOf(info), info, is(notNullValue()));
        return info;
    }

    static BroadcastInfo broadcast(SubPlan.Port container) {
        BroadcastInfo info = container.getAttribute(BroadcastInfo.class);
        assertThat(String.valueOf(info), info, is(notNullValue()));
        return info;
    }

    static Matcher<? super SubPlan> driverType(Matcher<? super DriverType> matcher) {
        return new FeatureMatcher<SubPlan, DriverType>(matcher, "driver type", "driver type") {
            @Override
            protected DriverType featureValueOf(SubPlan actual) {
                return info(actual).getDriverType();
            }
        };
    }

    static Matcher<? super SubPlan> driverOption(Matcher<? super DriverOption> matcher) {
        return new FeatureMatcher<SubPlan, Set<DriverOption>>(hasItem(matcher), "driver option", "driver option") {
            @Override
            protected Set<DriverOption> featureValueOf(SubPlan actual) {
                return info(actual).getDriverOptions();
            }
        };
    }

    static Matcher<? super SubPlan> primaryOperator(Matcher<? super Operator> matcher) {
        return new FeatureMatcher<SubPlan, Operator>(matcher, "primary operator", "primary operator") {
            @Override
            protected Operator featureValueOf(SubPlan actual) {
                return info(actual).getPrimaryOperator();
            }
        };
    }

    static Matcher<? super SubPlan.Input> inputType(Matcher<? super InputType> matcher) {
        return new FeatureMatcher<SubPlan.Input, InputType>(matcher, "input type", "input type") {
            @Override
            protected InputType featureValueOf(SubPlan.Input actual) {
                return info(actual).getInputType();
            }
        };
    }

    static Matcher<? super SubPlan.Input> inputOption(Matcher<? super InputOption> matcher) {
        return new FeatureMatcher<SubPlan.Input, Set<InputOption>>(hasItem(matcher), "input option", "input option") {
            @Override
            protected Set<InputOption> featureValueOf(SubPlan.Input actual) {
                return info(actual).getInputOptions();
            }
        };
    }

    static Matcher<? super SubPlan.Input> inputPartition(Matcher<? super Group> matcher) {
        return new FeatureMatcher<SubPlan.Input, Group>(matcher, "partition", "partition") {
            @Override
            protected Group featureValueOf(SubPlan.Input actual) {
                return info(actual).getPartitionInfo();
            }
        };
    }

    static Matcher<? super SubPlan.Output> outputType(Matcher<? super OutputType> matcher) {
        return new FeatureMatcher<SubPlan.Output, OutputType>(matcher, "output type", "output type") {
            @Override
            protected OutputType featureValueOf(SubPlan.Output actual) {
                return info(actual).getOutputType();
            }
        };
    }

    static Matcher<? super SubPlan.Output> outputOption(Matcher<? super OutputOption> matcher) {
        return new FeatureMatcher<SubPlan.Output, Set<OutputOption>>(hasItem(matcher), "output option", "output option") {
            @Override
            protected Set<OutputOption> featureValueOf(SubPlan.Output actual) {
                return info(actual).getOutputOptions();
            }
        };
    }

    static Matcher<? super SubPlan.Output> outputPartition(Matcher<? super Group> matcher) {
        return new FeatureMatcher<SubPlan.Output, Group>(matcher, "partition", "partition") {
            @Override
            protected Group featureValueOf(SubPlan.Output actual) {
                return info(actual).getPartitionInfo();
            }
        };
    }

    static Matcher<? super SubPlan.Output> outputAggregation(Matcher<? super Operator> matcher) {
        return new FeatureMatcher<SubPlan.Output, Operator>(matcher, "aggregator", "aggregator") {
            @Override
            protected Operator featureValueOf(SubPlan.Output actual) {
                return info(actual).getAggregationInfo();
            }
        };
    }

    static Matcher<? super SubPlan.Port> broadcastFormat(Matcher<? super Group> matcher) {
        return new FeatureMatcher<SubPlan.Port, Group>(matcher, "format", "format") {
            @Override
            protected Group featureValueOf(SubPlan.Port actual) {
                return broadcast(actual).getFormatInfo();
            }
        };
    }

    private static CoreOperator.Builder cp() {
        return CoreOperator.builder(CoreOperatorKind.CHECKPOINT);
    }

    private static UserOperator.Builder op(Class<? extends Annotation> annotation, String name) {
        return OperatorExtractor.extract(annotation, Ops.class, name);
    }

    private UserOperator.Builder newJoin(MockOperators m) {
        return op(MasterJoinUpdate.class, "join")
                .input("m", m.getCommonDataType(), group("+k"))
                .input("t", m.getCommonDataType(), group("+k"))
                .output("f", m.getCommonDataType())
                .output("m", m.getCommonDataType());
    }

    @SuppressWarnings("javadoc")
    public abstract static class Ops {

        @Extract
        public abstract void extract();

        @CoGroup
        public abstract void cogroup();

        @Fold(partialAggregation = PartialAggregation.PARTIAL)
        public abstract void fold_partial();

        @Fold(partialAggregation = PartialAggregation.TOTAL)
        public abstract void fold_total();

        @MasterJoinUpdate
        public abstract void join();
    }
}
