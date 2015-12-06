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

import org.junit.Test;

import com.asakusafw.spark.compiler.planning.IterativeInfo.RecomputeKind;

/**
 * Test for {@link IterativeInfo}.
 */
public class IterativeInfoTest extends PlanningTestRoot {

    /**
     * always.
     */
    @Test
    public void always() {
        IterativeInfo info = IterativeInfo.always();
        assertThat(info.getRecomputeKind(), is(RecomputeKind.ALWAYS));
        assertThat(info.getParameters(), hasSize(0));
    }

    /**
     * parameter.
     */
    @Test
    public void parameter() {
        IterativeInfo info = IterativeInfo.parameter("A", "B", "C");
        assertThat(info.getRecomputeKind(), is(RecomputeKind.PARAMETER));
        assertThat(info.getParameters(), containsInAnyOrder("A", "B", "C"));
    }

    /**
     * never.
     */
    @Test
    public void never() {
        IterativeInfo info = IterativeInfo.never();
        assertThat(info.getRecomputeKind(), is(RecomputeKind.NEVER));
        assertThat(info.getParameters(), hasSize(0));
    }

    /**
     * merge contains always.
     */
    @Test
    public void merge_always() {
        IterativeInfo always = IterativeInfo.always();
        IterativeInfo parameter = IterativeInfo.parameter("testing");
        IterativeInfo never = IterativeInfo.never();
        assertThat(always.merge(always), is(always));
        assertThat(always.merge(parameter), is(always));
        assertThat(always.merge(never), is(always));
        assertThat(parameter.merge(always), is(always));
        assertThat(never.merge(always), is(always));
    }

    /**
     * merge contains parameter.
     */
    @Test
    public void merge_parameter() {
        IterativeInfo p1 = IterativeInfo.parameter("a");
        IterativeInfo p2 = IterativeInfo.parameter("b");
        IterativeInfo p3 = IterativeInfo.parameter("a", "b");
        IterativeInfo never = IterativeInfo.never();
        assertThat(p1.merge(p1), is(p1));
        assertThat(p1.merge(p2), is(p3));
        assertThat(never.merge(p1), is(p1));
        assertThat(p1.merge(never), is(p1));
    }
}
