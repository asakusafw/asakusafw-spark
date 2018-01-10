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

import java.util.HashMap;
import java.util.Map;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.asakusafw.lang.compiler.model.info.ExternalInputInfo.DataSize;
import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.basic.BasicExternalInputEstimator;
import com.asakusafw.lang.compiler.optimizer.basic.BasicPropagateEstimator;
import com.asakusafw.lang.compiler.optimizer.basic.CompositeOperatorEstimator;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Test for {@link PlanEstimator}.
 */
public class PlanEstimatorTest extends PlanningTestRoot {

    private static final double SIZE_TINY = 10.0;

    private static final double SIZE_SMALL = 100.0;

    private static final double SIZE_LARGE = 1000.0;

    /**
     * simple case.
     */
    @Test
    public void simple() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
            .input("in", DataSize.TINY)
            .output("out").connect("in", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(2));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("out"));

        PlanEstimator estimator = estimator();
        SizeInfo e1 = estimator.estimate(input(s1));
        SizeInfo e0 = estimator.estimate(output(s0));
        assertThat(e0.toString(), e0, size(closeTo(SIZE_TINY, 0.0)));
        assertThat(e1.toString(), e1, size(closeTo(SIZE_TINY, 0.0)));
    }

    /**
     * multiple.
     */
    @Test
    public void multiple() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
            .input("in0", DataSize.TINY)
            .input("in1", DataSize.SMALL)
            .input("in2", DataSize.LARGE)
            .output("out")
                .connect("in0", "out")
                .connect("in1", "out")
                .connect("in2", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));
        SubPlan s2 = ownerOf(detail, mock.get("in2"));
        SubPlan s3 = ownerOf(detail, mock.get("out"));

        PlanEstimator estimator = estimator();
        assertThat(estimator.estimate(input(s3)), size(closeTo(SIZE_TINY + SIZE_SMALL + SIZE_LARGE, 0.0)));
        assertThat(estimator.estimate(output(s0)), size(closeTo(SIZE_TINY, 0.0)));
        assertThat(estimator.estimate(output(s1)), size(closeTo(SIZE_SMALL, 0.0)));
        assertThat(estimator.estimate(output(s2)), size(closeTo(SIZE_LARGE, 0.0)));
    }

    /**
     * includes unknown.
     */
    @Test
    public void unknown() {
        PlanDetail detail = SparkPlanning.plan(context(), new MockOperators()
            .input("in0", DataSize.TINY)
            .input("in1", DataSize.UNKNOWN)
            .output("out")
                .connect("in0", "out")
                .connect("in1", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        SubPlan s0 = ownerOf(detail, mock.get("out"));

        PlanEstimator estimator = estimator();
        SizeInfo e0 = estimator.estimate(input(s0));
        assertThat(e0.toString(), e0, size(is(Double.NaN)));
    }

    private PlanEstimator estimator(String... keyValuePairs) {
        PlanningContext context = context(keyValuePairs);
        Map<DataSize, Double> map = new HashMap<>();
        map.put(DataSize.TINY, SIZE_TINY);
        map.put(DataSize.SMALL, SIZE_SMALL);
        map.put(DataSize.LARGE, SIZE_LARGE);
        return new PlanEstimator(
                CompositeOperatorEstimator.builder()
                    .withDefault(new BasicPropagateEstimator())
                    .withInput(new BasicExternalInputEstimator(map))
                    .withOutput(OperatorEstimator.NULL)
                    .build(),
                context.getOptimizerContext());
    }

    static Matcher<? super SizeInfo> size(Matcher<? super Double> matcher) {
        return new FeatureMatcher<SizeInfo, Double>(matcher, "size of", "size of") {
            @Override
            protected Double featureValueOf(SizeInfo actual) {
                return actual.getSize();
            }
        };
    }
}
