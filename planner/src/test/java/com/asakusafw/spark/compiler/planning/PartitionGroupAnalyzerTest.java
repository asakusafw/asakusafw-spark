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

import java.util.Map;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.spark.compiler.planning.PartitionGroupInfo.DataSize;

/**
 * Test for {@link PartitionGroupAnalyzer}.
 */
public class PartitionGroupAnalyzerTest {

    /**
     * load limit map - simple case.
     */
    @Test
    public void load_simple() {
        CompilerOptions options = CompilerOptions.builder()
            .withProperty(key(DataSize.SMALL), String.valueOf(10L * 1024 * 1024))
            .build();
        Map<DataSize, Double> map = PartitionGroupAnalyzer.loadLimitMap(options);

        assertThat(map, hasKey(DataSize.TINY));
        assertThat(map, hasEntry(DataSize.SMALL, 10.0 * 1024 * 1024));
    }

    /**
     * load limit map - simple case.
     */
    @Test
    public void load_all() {
        CompilerOptions.Builder builder = CompilerOptions.builder();
        for (DataSize size : DataSize.values()) {
            builder.withProperty(key(size), String.valueOf(size.ordinal()));
        }
        Map<DataSize, Double> map = PartitionGroupAnalyzer.loadLimitMap(builder.build());
        for (DataSize size : DataSize.values()) {
            assertThat(map, hasEntry(size, (double) size.ordinal()));
        }
    }

    /**
     * load limit map - simple case.
     */
    @Test
    public void load_invalid() {
        CompilerOptions.Builder builder = CompilerOptions.builder();
        for (DataSize size : DataSize.values()) {
            builder.withProperty(key(size), String.valueOf("invalid"));
        }
        Map<DataSize, Double> map = PartitionGroupAnalyzer.loadLimitMap(builder.build());
        assertThat(map, is(PartitionGroupAnalyzer.DEFAULT_LIMITS));
    }

    private String key(DataSize size) {
        return PartitionGroupAnalyzer.KEY_LIMIT_PREFIX + size.getSymbol();
    }
}
