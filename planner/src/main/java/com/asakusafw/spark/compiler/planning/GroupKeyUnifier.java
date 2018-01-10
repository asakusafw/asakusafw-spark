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

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Provides {@link Group} keys for sub-plan inputs/outputs.
 */
public class GroupKeyUnifier {

    private final Map<SubPlan.Input, Group> inputs = new HashMap<>();

    private final Map<SubPlan.Output, Group> outputs = new HashMap<>();

    /**
     * Returns the group key for the target input.
     * @param port the target port
     * @return the group key, or {@code null} if the target port does not have key info
     */
    public Group get(SubPlan.Input port) {
        if (inputs.containsKey(port)) {
            return inputs.get(port);
        }
        Group result = computeInputGroup(port);
        inputs.put(port, result);
        return result;
    }

    /**
     * Returns the group key for the target output.
     * @param port the target port
     * @return the group key, or {@code null} if the target port does not have key info
     */
    public Group get(SubPlan.Output port) {
        if (outputs.containsKey(port)) {
            return outputs.get(port);
        }
        Group result = computeOutputGroup(port);
        outputs.put(port, result);
        return result;
    }

    private Group computeInputGroup(SubPlan.Input input) {
        boolean first = true;
        Group result = null;
        for (OperatorInput consumer : input.getOperator().getOutput().getOpposites()) {
            Group candidate = consumer.getGroup();
            if (first) {
                first = false;
                result = candidate;
            } else {
                result = unify(result, candidate);
            }
        }
        return result;
    }

    private Group computeOutputGroup(SubPlan.Output output) {
        boolean first = true;
        Group result = null;
        for (SubPlan.Input opposite : output.getOpposites()) {
            Group candidate = get(opposite);
            if (first) {
                first = false;
                result = candidate;
            } else {
                result = unify(result, candidate);
            }
        }
        return result;
    }

    private Group unify(Group current, Group replacement) {
        if (current == null) {
            if (replacement == null) {
                return null;
            }
        } else if (current.equals(replacement)) {
            return current;
        }
        throw new IllegalStateException(MessageFormat.format(
                "inconsistent group key: {0} <> {1}",
                current, replacement));
    }
}
