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

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.graph.UserOperator;

/**
 * Represents a user operator ID.
 */
public class OperatorId {

    private final ClassDescription operatorClass;

    private final String methodName;

    /**
     * Creates a new instance.
     * @param operatorClass the operator class
     * @param methodName the operator method name
     */
    public OperatorId(ClassDescription operatorClass, String methodName) {
        this.operatorClass = operatorClass;
        this.methodName = methodName;
    }

    /**
     * Returns the ID for the descriptor.
     * @param descriptor the operator descriptor
     * @return the corresponded ID
     * @throws IllegalArgumentException if descriptor is not valid
     */
    public static OperatorId of(String descriptor) {
        int separator = descriptor.lastIndexOf('.');
        if (separator <= 0 || separator == descriptor.length() - 1) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "invalid operator descriptor: {0}",
                    descriptor));
        }
        ClassDescription aClass = new ClassDescription(descriptor.substring(0, separator));
        String name = descriptor.substring(separator + 1);
        return new OperatorId(aClass, name);
    }

    /**
     * Returns the ID for the target operator.
     * @param operator the target operator
     * @return the corresponded ID
     */
    public static OperatorId of(UserOperator operator) {
        MethodDescription method = operator.getMethod();
        return new OperatorId(method.getDeclaringClass(), method.getName());
    }

    /**
     * Returns the operator class.
     * @return the operator class
     */
    public ClassDescription getOperatorClass() {
        return operatorClass;
    }

    /**
     * Returns the method name.
     * @return the method name
     */
    public String getMethodName() {
        return methodName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + methodName.hashCode();
        result = prime * result + operatorClass.hashCode();
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
        OperatorId other = (OperatorId) obj;
        if (!methodName.equals(other.methodName)) {
            return false;
        }
        if (!operatorClass.equals(other.operatorClass)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "{0}#{1}", //$NON-NLS-1$
                getOperatorClass().getClassName(),
                getMethodName());
    }
}
