/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.common.ComplexAttribute;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.spark.compiler.planning.IterativeInfo.RecomputeKind;

/**
 * Inferred parameters information.
 * @since 0.3.0
 */
public class ParameterInfo implements ComplexAttribute {

    private final Map<String, Set<Attribute>> parameters;

    /**
     * Creates a new instance.
     * @param parameters the parameters
     */
    public ParameterInfo(Map<String, ? extends Collection<Attribute>> parameters) {
        Map<String, Set<Attribute>> map = new LinkedHashMap<>();
        for (Map.Entry<String, ? extends Collection<Attribute>> entry : parameters.entrySet()) {
            map.put(entry.getKey(), Collections.unmodifiableSet(new LinkedHashSet<>(entry.getValue())));
        }
        this.parameters = Collections.unmodifiableMap(map);
    }

    /**
     * Creates a instance for the target plan.
     * @param plan the target plan
     * @return the created instance
     */
    public static ParameterInfo of(Plan plan) {
        Builder builder = new Builder();
        for (SubPlan element : plan.getElements()) {
            collect(element, builder);
        }
        return builder.build();
    }

    private static void collect(SubPlan element, Builder builder) {
        Set<Attribute> options = getOptions(element);
        for (Operator operator : element.getOperators()) {
            Set<String> parameters = collectParameters(operator);
            for (String parameter : parameters) {
                builder.add(parameter, Attribute.MANDATORY);
                builder.add(parameter, options);
            }
        }
    }

    private static Set<String> collectParameters(Operator operator) {
        Set<String> results = new HashSet<>();
        IterativeInfo info = IterativeInfo.getDeclared(operator);
        if (info.getRecomputeKind() == RecomputeKind.PARAMETER) {
            results.addAll(info.getParameters());
        }
        switch (operator.getOperatorKind()) {
        case INPUT:
            results.addAll(collectParameters((ExternalInput) operator));
            break;
        case OUTPUT:
            results.addAll(collectParameters((ExternalOutput) operator));
            break;
        default:
            break;
        }
        return results;
    }

    private static Set<String> collectParameters(ExternalInput operator) {
        ExternalInputInfo info = operator.getInfo();
        if (info == null) {
            return Collections.emptySet();
        }
        return info.getParameterNames();
    }

    private static Set<String> collectParameters(ExternalOutput operator) {
        ExternalOutputInfo info = operator.getInfo();
        if (info == null) {
            return Collections.emptySet();
        }
        return info.getParameterNames();
    }

    private static Set<Attribute> getOptions(SubPlan element) {
        IterativeInfo info = element.getAttribute(IterativeInfo.class);
        if (info == null) {
            return Collections.singleton(Attribute.FIXED);
        }
        if (isOutputVertex(element)) {
            return Collections.singleton(Attribute.FIXED);
        }
        return Collections.singleton(Attribute.ITERATIVE);
    }

    private static boolean isOutputVertex(SubPlan element) {
        for (Operator operator : element.getOperators()) {
            switch (operator.getOperatorKind()) {
            case MARKER:
            case OUTPUT:
                break;
            default:
                return false;
            }
        }
        return true;
    }

    @Override
    public Map<String, ?> toMap() {
        return parameters;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "ParameterInfo{0}", //$NON-NLS-1$
                toMap());
    }

    /**
     * The parameter attribute.
     * @since 0.3.0
     */
    public enum Attribute {

        /**
         * The target parameter is mandatory.
         */
        MANDATORY,

        /**
         * The target parameter will be used in fixed (non-iterative, non-continuous) context.
         */
        FIXED,

        /**
         * The target parameter will be used in iterative context.
         */
        ITERATIVE,
    }

    /**
     * A builder for {@link ParameterInfo}.
     * @since 0.3.0
     */
    public static class Builder {

        private final Map<String, Set<Attribute>> parameters = new LinkedHashMap<>();

        /**
         * Creates a new instance.
         */
        public Builder() {
            return;
        }

        /**
         * Adds a parameter and its attributes.
         * @param name the parameter name
         * @param attributes the attributes
         * @return this
         */
        public Builder add(String name, Attribute... attributes) {
            return add(name, Arrays.asList(attributes));
        }

        /**
         * Adds a parameter and its attributes.
         * @param name the parameter name
         * @param attributes the attributes
         * @return this
         */
        public Builder add(String name, Collection<Attribute> attributes) {
            Set<Attribute> attrs = parameters.get(name);
            if (attrs == null) {
                attrs = EnumSet.noneOf(Attribute.class);
                parameters.put(name, attrs);
            }
            attrs.addAll(attributes);
            return this;
        }

        /**
         * Builds an instance.
         * @return the built instance
         */
        public ParameterInfo build() {
            return new ParameterInfo(parameters);
        }
    }
}
