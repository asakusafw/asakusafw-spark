/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.spark.bootstrap;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Represents a bootstrap context.
 * @since 0.5.0
 */
public class Context {

    private static final int IDX_BATCH_ID = 0;

    private static final int IDX_FLOW_ID = 1;

    private static final int IDX_EXECUTION_ID = 2;

    private static final int IDX_BATCH_ARGUMENTS = 3;

    private static final int IDX_APPLICATON_CLASS_NAME = 4;

    private static final int IDX_EXTRA_ARGUMENTS_BEGIN = 5;

    private final String applicationClassName;

    private final String batchId;

    private final String flowId;

    private final String executionId;

    private final String batchArguments;

    private final List<String> extraArguments;

    /**
     * Creates a new instance.
     * @param applicationClassName the application class name
     * @param batchId the batch ID
     * @param flowId the flow ID
     * @param executionId the execution ID
     * @param batchArguments the batch arguments
     * @param extraArguments the extra arguments
     */
    public Context(
            String applicationClassName,
            String batchId, String flowId, String executionId,
            String batchArguments, List<String> extraArguments) {
        this.applicationClassName = applicationClassName;
        this.batchId = batchId;
        this.flowId = flowId;
        this.executionId = executionId;
        this.batchArguments = batchArguments;
        this.extraArguments = Collections.unmodifiableList(new ArrayList<>(extraArguments));
    }

    /**
     * Parses the program arguments.
     * @param args the program arguments
     * @return this
     */
    public static Context parse(String... args) {
        if (args.length < 5) {
            throw new ConfigurationException(MessageFormat.format(
                    "invalid arguments: {0}",
                    String.join(", ", args)));
        }
        return new Context(
                args[IDX_APPLICATON_CLASS_NAME],
                args[IDX_BATCH_ID],
                args[IDX_FLOW_ID],
                args[IDX_EXECUTION_ID],
                args[IDX_BATCH_ARGUMENTS],
                Arrays.asList(args).subList(IDX_EXTRA_ARGUMENTS_BEGIN, args.length));
    }

    /**
     * Returns the application class name.
     * @return the application class name
     */
    public String getApplicationClassName() {
        return applicationClassName;
    }

    /**
     * Returns the batch ID.
     * @return the batch ID
     */
    public String getBatchId() {
        return batchId;
    }

    /**
     * Returns the flow ID.
     * @return the flow ID
     */
    public String getFlowId() {
        return flowId;
    }

    /**
     * Returns the execution ID.
     * @return the execution ID
     */
    public String getExecutionId() {
        return executionId;
    }

    /**
     * Returns the serialized batch arguments.
     * @return the serialized batch arguments
     */
    public String getBatchArguments() {
        return batchArguments;
    }

    /**
     * Returns the extra arguments.
     * @return the extra arguments
     */
    public List<String> getExtraArguments() {
        return extraArguments;
    }
}
