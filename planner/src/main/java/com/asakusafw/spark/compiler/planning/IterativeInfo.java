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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.asakusafw.lang.compiler.common.AttributeContainer;
import com.asakusafw.lang.compiler.common.ComplexAttribute;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.iterative.IterativeExtension;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Information about iterative extensions.
 * @since 0.3.0
 */
public class IterativeInfo implements ComplexAttribute {

    private static final IterativeInfo CONST_ALWAYS = new IterativeInfo(RecomputeKind.ALWAYS);

    private static final IterativeInfo CONST_NEVER = new IterativeInfo(RecomputeKind.NEVER);

    private final RecomputeKind recomputeKind;

    private final Set<String> parameters;

    /**
     * Creates a new instance.
     * @param kind the re-compute kind
     */
    protected IterativeInfo(RecomputeKind kind) {
        this.recomputeKind = kind;
        this.parameters = Collections.emptySet();
    }

    /**
     * Creates a new instance.
     * @param kind the re-compute kind
     * @param parameters scope parameters
     */
    protected IterativeInfo(RecomputeKind kind, Collection<String> parameters) {
        this.recomputeKind = RecomputeKind.PARAMETER;
        this.parameters = Collections.unmodifiableSet(new HashSet<>(parameters));
    }

    /**
     * Returns an object which requires always re-compute the target node or port.
     * @return the object
     */
    public static IterativeInfo always() {
        return CONST_ALWAYS;
    }

    /**
     * Returns an object which never requires re-compute the target node or port.
     * @return the object
     */
    public static IterativeInfo never() {
        return CONST_NEVER;
    }

    /**
     * Returns an object which requires re-compute the target node or port if any parameters were changed.
     * @param parameters the corresponding parameter names
     * @return the object
     */
    public static IterativeInfo parameter(String... parameters) {
        Objects.requireNonNull(parameters);
        return parameter(Arrays.asList(parameters));
    }

    /**
     * Returns an object which requires re-compute the target node or port if any parameters were changed.
     * @param parameters the corresponding parameter names
     * @return the object
     */
    public static IterativeInfo parameter(Collection<String> parameters) {
        Objects.requireNonNull(parameters);
        return new IterativeInfo(RecomputeKind.PARAMETER, parameters);
    }

    /**
     * Merges two objects.
     * @param other the another object
     * @return the merged object
     */
    public IterativeInfo merge(IterativeInfo other) {
        Objects.requireNonNull(other);
        if (this.equals(other)) {
            return this;
        } else if (getRecomputeKind() == RecomputeKind.ALWAYS
                || other.getRecomputeKind() == RecomputeKind.ALWAYS) {
            return always();
        } else if (getRecomputeKind() == RecomputeKind.PARAMETER
                || other.getRecomputeKind() == RecomputeKind.PARAMETER) {
            if (getRecomputeKind() != RecomputeKind.PARAMETER) {
                return other;
            }
            if (other.getRecomputeKind() != RecomputeKind.PARAMETER) {
                return this;
            }
            assert getRecomputeKind() == RecomputeKind.PARAMETER;
            assert other.getRecomputeKind() == RecomputeKind.PARAMETER;
            Set<String> params = new LinkedHashSet<>();
            params.addAll(getParameters());
            params.addAll(other.getParameters());
            return parameter(params);
        }
        // may not come here
        return never();
    }

    /**
     * Returns this information indicates whether the target is iterative element or not.
     * @return {@code true} if the target element is iterative, otherwise {@code false}
     */
    public boolean isIterative() {
        return recomputeKind != RecomputeKind.NEVER;
    }

    /**
     * Returns the re-compute kind for each iteration.
     * @return  the re-compute kind
     */
    public RecomputeKind getRecomputeKind() {
        return recomputeKind;
    }

    /**
     * Returns the iterative parameter names (only for {@link RecomputeKind#PARAMETER}).
     * @return the iterative parameter names
     */
    public Set<String> getParameters() {
        return parameters;
    }

    /**
     * Returns whether the target plan is iterative or not.
     * @param plan the target plan
     * @return {@code true} if the target plan is iterative, otherwise {@code false}
     */
    public static boolean isIterative(Plan plan) {
        Objects.requireNonNull(plan);
        IterativeInfo info = plan.getAttribute(IterativeInfo.class);
        return info != null && info.getRecomputeKind() != RecomputeKind.NEVER;
    }

    /**
     * Returns information about iterative operation for the target plan.
     * @param element the target element
     * @return the corresponded information
     */
    public static IterativeInfo get(Plan element) {
        return getInfo(element);
    }

    /**
     * Returns information about iterative operation for the target sub-plan.
     * @param element the target element
     * @return the corresponded information
     */
    public static IterativeInfo get(SubPlan element) {
        return getInfo(element);
    }

    /**
     * Returns information about iterative operation for the target sub-plan input.
     * @param element the target element
     * @return the corresponded information
     */
    public static IterativeInfo get(SubPlan.Input element) {
        return getInfo(element);
    }

    /**
     * Returns information about iterative operation for the target sub-plan output.
     * @param element the target element
     * @return the corresponded information
     */
    public static IterativeInfo get(SubPlan.Output element) {
        return getInfo(element);
    }

    static IterativeInfo getInfo(AttributeContainer element) {
        Objects.requireNonNull(element);
        IterativeInfo info = element.getAttribute(IterativeInfo.class);
        if (info == null) {
            throw new IllegalStateException();
        }
        return info;
    }

    /**
     * Returns NON-TRANSITIVE information about iterative operation for the target operator.
     * Be careful that this DOES NOT consider whether the operator upstreams are iterative or not.
     * @param element the target element
     * @return the corresponded information
     */
    public static IterativeInfo getDeclared(Operator element) {
        Objects.requireNonNull(element);
        IterativeExtension attribute = element.getAttribute(IterativeExtension.class);
        if (attribute == null) {
            return IterativeInfo.never();
        }
        Set<String> parameters = attribute.getParameters();
        if (parameters.isEmpty()) {
            return IterativeInfo.always();
        }
        return IterativeInfo.parameter(parameters);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Objects.hashCode(recomputeKind);
        result = prime * result + Objects.hashCode(parameters);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IterativeInfo other = (IterativeInfo) obj;
        if (recomputeKind != other.recomputeKind) {
            return false;
        }
        if (!Objects.equals(parameters, other.parameters)) {
            return false;
        }
        return true;
    }

    @Override
    public Map<String, ?> toMap() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("recompute", getRecomputeKind()); //$NON-NLS-1$
        if (getRecomputeKind() == RecomputeKind.PARAMETER) {
            results.put("parameters", getParameters()); //$NON-NLS-1$
        }
        return results;
    }

    @Override
    public String toString() {
        return toMap().toString();
    }

    /**
     * Represents when the target node or port requires re-compute for each iteration.
     * @since 0.3.0
     */
    public enum RecomputeKind {

        /**
         * Recompute is always required.
         */
        ALWAYS,

        /**
         * Recompute is required only if any {@link IterativeInfo#getParameters() parameters} were changed.
         */
        PARAMETER,

        /**
         * Recompute is never required.
         */
        NEVER,
    }
}
