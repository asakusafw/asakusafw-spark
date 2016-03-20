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
package com.asakusafw.spark.testdriver.adapter;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.extension.redirector.RedirectorParticipant;
import com.asakusafw.lang.compiler.testdriver.adapter.CompilerProfileInitializer;
import com.asakusafw.lang.compiler.tester.CompilerProfile;
import com.asakusafw.testdriver.compiler.CompilerConfiguration;

/**
 * {@link CompilerProfileInitializer} for Spark.
 */
public class SparkCompilerProfileInitializer implements CompilerProfileInitializer {

    private static final Map<String, String> REDIRECT_MAP;
    static {
        Map<String, String> map = new LinkedHashMap<>();
        putRedirect(map,
                "com.asakusafw.runtime.core.BatchContext", //$NON-NLS-1$
                "com.asakusafw.bridge.api.BatchContext"); //$NON-NLS-1$
        putRedirect(map,
                "com.asakusafw.runtime.core.Report", //$NON-NLS-1$
                "com.asakusafw.bridge.api.Report"); //$NON-NLS-1$
        putRedirect(map,
                "com.asakusafw.runtime.directio.api.DirectIo", //$NON-NLS-1$
                "com.asakusafw.bridge.directio.api.DirectIo"); //$NON-NLS-1$
        REDIRECT_MAP = Collections.unmodifiableMap(map);
    }

    private static void putRedirect(Map<String, String> target, String from, String to) {
        target.put(RedirectorParticipant.KEY_RULE_PREFIX + from, to);
    }

    @Override
    public void initialize(CompilerProfile profile, CompilerConfiguration configuration) {
        installOptions(REDIRECT_MAP, profile, configuration);
    }

    private void installOptions(
            Map<String, String> options,
            CompilerProfile profile,
            CompilerConfiguration configuration) {
        for (Map.Entry<String, String> entry : REDIRECT_MAP.entrySet()) {
            String key = entry.getKey();
            if (configuration.getOptions().containsKey(key) == false) {
                profile.forCompilerOptions().withProperty(key, entry.getValue());
            }
        }
    }
}
