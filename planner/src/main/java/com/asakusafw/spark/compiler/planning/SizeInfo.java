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

import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Estimated data-set size information.
 */
public class SizeInfo {

    /**
     * Represents information of not sure.
     */
    public static final SizeInfo UNKNOWN = new SizeInfo(Double.NaN);

    private final double size;

    /**
     * Creates a new instance.
     * @param size the estimated data-set size in bytes
     */
    public SizeInfo(double size) {
        this.size = size;
    }

    /**
     * Returns the estimated size.
     * @return the estimated size (in bytes), or {@code Double.NaN} if the target size is not sure
     */
    public double getSize() {
        return size;
    }

    /**
     * Returns the estimated size for the target sub-plan input/output.
     * @param port the target port
     * @return the estimated size (in bytes), or {@code Double.NaN} if the target size is not sure
     */
    public static double getSize(SubPlan.Port port) {
        SizeInfo info = port.getAttribute(SizeInfo.class);
        if (info != null) {
            return info.getSize();
        }
        return Double.NaN;
    }

    @Override
    public String toString() {
        if (Double.isNaN(size)) {
            return "N/A";
        } else if (Double.isInfinite(size)) {
            return String.valueOf(size);
        } else {
            return String.format("%,.0fbytes", size); //$NON-NLS-1$
        }
    }
}
