/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import org.junit.Test;

import com.asakusafw.lang.compiler.model.iterative.IterativeExtension;
import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Test for {@link IterativeOperationAnalyzer}.
 */
public class IterativeOperationAnalyzerTest extends PlanningTestRoot {

    /**
     * simple case.
<pre>{@code
in[*] --- out
==>
in[*] --- *C[*] --- out[*]
}</pre>
     */
    @Test
    public void simple() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
                .input("in", new IterativeExtension())
                .output("out").connect("in", "out")
                .toGraph());
        Plan plan = detail.getPlan();
        assertThat(IterativeInfo.isIterative(plan), is(true));
        IterativeInfo info = IterativeInfo.get(plan);
        assertThat(info, is(IterativeInfo.always()));

        MockOperators mock = restore(detail);
        assertThat(plan.getElements(), hasSize(2));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("out"));

        assertThat(IterativeInfo.get(s0), is(IterativeInfo.always()));
        assertThat(IterativeInfo.get(output(s0)), is(IterativeInfo.always()));

        assertThat(IterativeInfo.get(s1), is(IterativeInfo.always()));
        assertThat(IterativeInfo.get(input(s1)), is(IterativeInfo.always()));
    }

    /**
     * not iterative.
<pre>{@code
in[-] --- out
==>
in[-] --- *C[-] --- out[-]
}</pre>
     */
    @Test
    public void non_iterative() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
                .input("in")
                .output("out").connect("in", "out")
                .toGraph());
        Plan plan = detail.getPlan();
        assertThat(IterativeInfo.isIterative(plan), is(false));
    }

    /**
     * confluent iteratives.
<pre>{@code
in1[a] ---+ out
in2[b] --/
==>
in1[a] ---+ *C[a,b] --- out[*]
in2[b] --/
}</pre>
     */
    @Test
    public void confluent_iterative() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
                .input("in1", new IterativeExtension("a"))
                .input("in2", new IterativeExtension("b"))
                .output("out")
                    .connect("in1", "out")
                    .connect("in2", "out")
                .toGraph());
        Plan plan = detail.getPlan();
        assertThat(IterativeInfo.isIterative(plan), is(true));
        IterativeInfo info = IterativeInfo.get(plan);
        assertThat(info, is(IterativeInfo.always()));

        MockOperators mock = restore(detail);
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in1"));
        SubPlan s1 = ownerOf(detail, mock.get("in2"));
        SubPlan s2 = ownerOf(detail, mock.get("out"));

        assertThat(IterativeInfo.get(s0), is(IterativeInfo.parameter("a")));
        assertThat(IterativeInfo.get(output(s0)), is(IterativeInfo.parameter("a")));

        assertThat(IterativeInfo.get(s1), is(IterativeInfo.parameter("b")));
        assertThat(IterativeInfo.get(output(s1)), is(IterativeInfo.parameter("b")));

        assertThat(IterativeInfo.get(s2), is(IterativeInfo.always()));
        assertThat(IterativeInfo.get(input(s2)), is(IterativeInfo.parameter("a", "b")));
    }
    /**
     * confluent iteratives.
<pre>{@code
in1[a] ---+ out
in2[-] --/
==>
in1[a] ---+ *C[a] --- out[*]
in2[-] --/
}</pre>
     */
    @Test
    public void confluent_mixed() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
                .input("in1", new IterativeExtension("a"))
                .input("in2")
                .output("out")
                    .connect("in1", "out")
                    .connect("in2", "out")
                .toGraph());
        Plan plan = detail.getPlan();
        assertThat(IterativeInfo.isIterative(plan), is(true));
        IterativeInfo info = IterativeInfo.get(plan);
        assertThat(info, is(IterativeInfo.always()));

        MockOperators mock = restore(detail);
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in1"));
        SubPlan s1 = ownerOf(detail, mock.get("in2"));
        SubPlan s2 = ownerOf(detail, mock.get("out"));

        assertThat(IterativeInfo.get(s0), is(IterativeInfo.parameter("a")));
        assertThat(IterativeInfo.get(output(s0)), is(IterativeInfo.parameter("a")));

        assertThat(IterativeInfo.get(s1), is(IterativeInfo.never()));
        assertThat(IterativeInfo.get(output(s1)), is(IterativeInfo.never()));

        assertThat(IterativeInfo.get(s2), is(IterativeInfo.always()));
        assertThat(IterativeInfo.get(input(s2)), is(IterativeInfo.parameter("a")));
    }
}
