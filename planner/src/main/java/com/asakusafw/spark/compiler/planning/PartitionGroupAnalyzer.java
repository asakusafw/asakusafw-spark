/**
 * Copyright 2011-2016 Asakusa Framework Team.
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

import static com.asakusafw.spark.compiler.planning.Util.*;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.common.util.StringUtil;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.spark.compiler.planning.PartitionGroupInfo.DataSize;
import com.asakusafw.spark.compiler.planning.SubPlanInputInfo.InputType;

/**
 * Analyzes sub-plan inputs/outputs and provides {@link PartitionGroupInfo}.
 * This requires following extra information for each sub-plan input:
 * <ul>
 * <li> {@link SizeInfo} </li>
 * <li> {@link SubPlanInputInfo} </li>
 * </ul>
 */
public class PartitionGroupAnalyzer {

    static final Logger LOG = LoggerFactory.getLogger(PartitionGroupAnalyzer.class);

    static final String KEY_BASE_PREFIX = "spark.parallelism."; //$NON-NLS-1$

    /**
     * The compiler property key prefix of data size limits for each {@link DataSize} scale.
     */
    public static final String KEY_LIMIT_PREFIX = KEY_BASE_PREFIX + "limit."; //$NON-NLS-1$

    /**
     * The compiler property key prefix of explicit data size scales of each Asakusa operator.
     */
    public static final String KEY_OPERATOR_PREFIX = KEY_BASE_PREFIX + "operator."; //$NON-NLS-1$

    static final Map<DataSize, Double> DEFAULT_LIMITS;
    static {
        Map<DataSize, Double> map = new HashMap<>();
        map.put(DataSize.TINY, Double.valueOf(20.0 * 1024 * 1024));
        map.put(DataSize.SMALL, Double.NaN);
        map.put(DataSize.REGULAR, Double.POSITIVE_INFINITY);
        map.put(DataSize.LARGE, Double.POSITIVE_INFINITY);
        map.put(DataSize.HUGE, Double.POSITIVE_INFINITY);
        DEFAULT_LIMITS = EnumUtil.freeze(map);
    }

    private static final Set<InputType> TARGET_INPUT_TYPES = EnumUtil.freeze(InputType.PARTITIONED);

    final EnumMap<DataSize, Double> limits;

    final Map<SubPlan, DataSize> explicits;

    private final Map<SubPlan.Input, PartitionGroupInfo> cache = new HashMap<>();

    /**
     * Returns data size limit map from the compiler options.
     * @param options the compiler options
     * @return the data size limit map
     */
    public static Map<DataSize, Double> loadLimitMap(CompilerOptions options) {
        Map<DataSize, Double> results = new HashMap<>(DEFAULT_LIMITS);
        for (DataSize size : DataSize.values()) {
            String key = KEY_LIMIT_PREFIX + size.getSymbol();
            String value = options.get(key, StringUtil.EMPTY).trim();
            if (value.isEmpty()) {
                continue;
            }
            try {
                double bytes = Double.parseDouble(value);
                results.put(size, bytes);
                LOG.debug("spark parallel: {}={}", size, bytes); //$NON-NLS-1$
            } catch (NumberFormatException e) {
                LOG.warn(MessageFormat.format(
                        "invalid data size: {0}={1}",
                        key,
                        value));
            }
        }
        return EnumUtil.freeze(results);
    }

    /**
     * Returns the explicit data size map of each sub-plan from the compiler options.
     * @param options the compiler options
     * @param plan the target execution plan
     * @return the explicit data size map
     */
    public static Map<SubPlan, DataSize> loadExplicitSizeMap(CompilerOptions options, Plan plan) {
        Map<OperatorId, DataSize> sizeMap = extractExplicitOperatorSizeMap(options);
        if (sizeMap.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<SubPlan, DataSize> results = new LinkedHashMap<>();
        for (SubPlan sub : plan.getElements()) {
            DataSize size = loadExplicitSize(sizeMap, sub);
            if (size != null) {
                results.put(sub, size);
            }
        }
        return Collections.unmodifiableMap(results);
    }

    private static Map<OperatorId, DataSize> extractExplicitOperatorSizeMap(CompilerOptions options) {
        Map<OperatorId, DataSize> results = new HashMap<>();
        for (Map.Entry<String, String> entry : options.getProperties(KEY_OPERATOR_PREFIX).entrySet()) {
            String raw = entry.getKey();
            String key = raw.substring(KEY_OPERATOR_PREFIX.length());
            String value = entry.getValue();

            OperatorId id;
            try {
                id = OperatorId.of(key);
            } catch (IllegalArgumentException e) {
                LOG.warn(MessageFormat.format(
                        "invalid operator descriptor: {0} ({1})",
                        key, raw), e);
                continue;
            }

            DataSize size = DataSize.find(value);
            if (size == null) {
                LOG.warn(MessageFormat.format(
                        "invalid operator size: {0} ({1})",
                        value, raw));
                continue;
            }

            results.put(id, size);
        }
        return results;
    }

    /**
     * Returns the explicit data size of the sub-plan.
     * @param sizeMap explicit data size of each operator
     * @param sub the target sub-plan
     * @return the explicit data size for the sub-plan, or {@code null} if it is not defined
     */
    public static DataSize loadExplicitSize(Map<OperatorId, DataSize> sizeMap, SubPlan sub) {
        DataSize max = null;
        for (OperatorId id : collectOperatorIds(sub)) {
            DataSize size = sizeMap.get(id);
            if (size == null) {
                continue;
            }
            LOG.debug("explicit size: {} -> {}", size, id);
            if (max == null || size.compareTo(max) > 0) {
                max = size;
            }
        }
        return max;
    }

    /**
     * Creates a new instance with default configuration.
     */
    public PartitionGroupAnalyzer() {
        this(DEFAULT_LIMITS, Collections.<SubPlan, DataSize>emptyMap());
    }

    /**
     * Creates a new instance.
     * @param limits the limit map
     * @param explicitSizeMap the explicit data size map
     */
    public PartitionGroupAnalyzer(Map<DataSize, Double> limits, Map<? extends SubPlan, DataSize> explicitSizeMap) {
        EnumMap<DataSize, Double> map = new EnumMap<>(DataSize.class);
        map.putAll(limits);
        this.limits = map;
        this.explicits = Collections.unmodifiableMap(new HashMap<>(explicitSizeMap));
    }

    /**
     * Returns {@link PartitionGroupInfo} for the target sub-plan input.
     * @param port the target input port
     * @return the analyzed result, or {@code null} if the target port is not in any partition groups
     */
    public PartitionGroupInfo analyze(SubPlan.Input port) {
        if (isSupported(port) == false) {
            return null;
        }
        return analyze0(port);
    }

    /**
     * Returns {@link PartitionGroupInfo} for the target sub-plan output.
     * @param port the target output port
     * @return the analyzed result, or {@code null} if the target port is not in any partition groups
     */
    public PartitionGroupInfo analyze(SubPlan.Output port) {
        List<SubPlan.Input> supported = onlySupported(port.getOpposites());
        if (supported.isEmpty()) {
            return null;
        }
        return analyze0(supported.get(0));
    }

    private PartitionGroupInfo analyze0(SubPlan.Input port) {
        assert isSupported(port);
        if (cache.containsKey(port)) {
            return cache.get(port);
        }
        Collector collector = new Collector();
        collector.add(port);

        DataSize size = collector.getDataSize();
        PartitionGroupInfo info = new PartitionGroupInfo(size);
        for (SubPlan.Input p : collector.getMembers()) {
            assert cache.containsKey(p) == false;
            cache.put(p, info);
        }
        assert cache.containsKey(port);
        return info;
    }

    static List<SubPlan.Input> onlySupported(Collection<? extends SubPlan.Input> inputs) {
        List<SubPlan.Input> results = new ArrayList<>();
        for (SubPlan.Input input : inputs) {
            if (isSupported(input)) {
                results.add(input);
            }
        }
        return results;
    }

    static boolean isSupported(SubPlan.Input port) {
        SubPlanInputInfo info = port.getAttribute(SubPlanInputInfo.class);
        if (info == null) {
            return false;
        }
        return TARGET_INPUT_TYPES.contains(info.getInputType());
    }

    private class Collector {

        final Set<SubPlan> consumers = new HashSet<>();

        Collector() {
            return;
        }

        public void add(SubPlan.Input start) {
            assert isSupported(start);
            Set<SubPlan> saw = new HashSet<>(consumers);
            LinkedList<SubPlan> work = new LinkedList<>();
            work.add(start.getOwner());
            while (work.isEmpty() == false) {
                SubPlan next = work.removeFirst();
                if (saw.contains(next)) {
                    continue;
                }
                saw.add(next);
                for (SubPlan.Input input : onlySupported(next.getInputs())) {
                    for (SubPlan.Output upstream : input.getOpposites()) {
                        for (SubPlan.Input downstream : upstream.getOpposites()) {
                            // NOTE: the downstream may be always supported
                            if (isSupported(downstream)) {
                                work.addFirst(downstream.getOwner());
                            }
                        }
                    }
                }
            }
            consumers.addAll(saw);
        }

        public DataSize getDataSize() {
            DataSize explicit = getExplicitDataSize();
            if (explicit != null) {
                return explicit;
            }
            return getInferedDataSize();
        }

        private DataSize getExplicitDataSize() {
            DataSize current = null;
            for (SubPlan sub : consumers) {
                DataSize size = explicits.get(sub);
                if (size == null) {
                    continue;
                }
                if (current == null || size.isLargetThan(current)) {
                    current = size;
                }
            }
            return current;
        }

        private DataSize getInferedDataSize() {
            DataSize current = null;
            for (SubPlan sub : consumers) {
                DataSize size = computeDataSize(sub);
                if (current == null || size.isLargetThan(current)) {
                    current = size;
                }
            }
            return current == null ? DataSize.REGULAR : current;
        }

        private DataSize computeDataSize(SubPlan sub) {
            boolean sawNaN = false;
            double total = 0.0;
            for (SubPlan.Input input : onlySupported(sub.getInputs())) {
                double size = SizeInfo.getSize(input);
                if (Double.isNaN(size)) {
                    sawNaN = true;
                } else {
                    total += Math.max(0.0, size);
                }
            }
            for (Map.Entry<DataSize, Double> entry : limits.entrySet()) {
                if (entry.getValue() == null) {
                    continue;
                }
                DataSize scale = entry.getKey();
                if (sawNaN && DataSize.REGULAR.isLargetThan(scale)) {
                    continue;
                }
                double value = entry.getValue();
                if (Double.isNaN(value)) {
                    continue;
                }
                if (total <= value) {
                    return scale;
                }
            }
            // unknown data size
            return DataSize.REGULAR;
        }

        public Set<SubPlan.Input> getMembers() {
            Set<SubPlan.Input> results = new HashSet<>();
            for (SubPlan sub : consumers) {
                results.addAll(onlySupported(sub.getInputs()));
            }
            return results;
        }
    }
}
