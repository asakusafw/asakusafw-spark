package com.asakusafw.lang.compiler.model.graph;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.ImmediateDescription;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.description.ReifiableTypeDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription.ValueKind;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.FlowOperator;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorArgument;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;

/**
 * Mock operator graph builder.
 */
public final class MockOperators {

    private static final TypeDescription TYPE = Descriptions.typeOf(String.class);

    private static final OperatorConstraint[] EMPTY_CONSTRAINTS = new OperatorConstraint[0];

    private static final AnnotationDescription ANNOTATION = new AnnotationDescription(
            classOf(Deprecated.class),
            Collections.<String, ValueDescription>emptyMap());

    private static final String KEY_ARGUMENT = "ID";

    private final Map<String, Operator> operators = new HashMap<>();

    /**
     * Creates a new instance.
     */
    public MockOperators() {
        return;
    }

    /**
     * Creates a new instance.
     * @param operators operators which are created on other {@link MockOperators}
     */
    public MockOperators(Collection<? extends Operator> operators) {
        for (Operator operator : operators) {
            String id = id0(operator);
            if (id != null) {
                this.operators.put(id, operator);
            }
        }
    }

    /**
     * Adds {@link ExternalInput}.
     * @param id the operator ID
     * @return this
     */
    public MockOperators input(String id) {
        operators.put(id, ExternalInput.newInstance(id, TYPE));
        return this;
    }

    /**
     * Adds {@link ExternalInput}.
     * @param id the operator ID
     * @return this
     */
    public MockOperators output(String id) {
        operators.put(id, ExternalOutput.newInstance(id, TYPE));
        return this;
    }

    /**
     * Adds {@link Operator} with a single input and output.
     * @param id the operator ID
     * @return this
     */
    public MockOperators operator(String id) {
        return operator(id, "in", "out", EMPTY_CONSTRAINTS);
    }

    /**
     * Adds {@link Operator} with a single input and output.
     * @param id the operator ID
     * @param constraints operator constraints
     * @return this
     */
    public MockOperators operator(String id, OperatorConstraint... constraints) {
        return operator(id, "in", "out", constraints);
    }

    /**
     * Adds {@link Operator}.
     * @param id the operator ID
     * @param inputNames comma separated input names
     * @param outputNames comma separated output names
     * @param constraints operator constraints
     * @return this
     */
    public MockOperators operator(String id, String inputNames, String outputNames, OperatorConstraint... constraints) {
        UserOperator.Builder builder = UserOperator.builder(
                ANNOTATION,
                new MethodDescription(
                        classOf(MockOperators.class), id, Collections.<ReifiableTypeDescription>emptyList()),
                classOf(MockOperators.class));
        for (String name : inputNames.split(",")) {
            builder.input(name, TYPE);
        }
        for (String name : outputNames.split(",")) {
            builder.output(name, TYPE);
        }
        builder.constraint(constraints);
        builder.argument(KEY_ARGUMENT, valueOf(id));
        return bless(id, builder.build());
    }

    /**
     * Adds a {@link FlowOperator}.
     * @param id the operator ID
     * @param subGraph internal graph
     * @return this
     */
    public MockOperators flow(String id, OperatorGraph subGraph) {
        FlowOperator.Builder builder = FlowOperator.builder(new ClassDescription(id), subGraph);
        for (ExternalInput port : subGraph.getInputs().values()) {
            builder.input(port.getName(), port.getDataType());
        }
        for (ExternalOutput port : subGraph.getOutputs().values()) {
            builder.output(port.getName(), port.getDataType());
        }
        return bless(id, builder.build());
    }

    /**
     * Adds {@link MarkerOperator}.
     * @param id the operator ID
     * @return this
     */
    public MockOperators marker(String id) {
        MarkerOperator operator = MarkerOperator.builder(TYPE)
                .attribute(String.class, id)
                .build();
        return bless(id, operator);
    }

    /**
     * Adds {@link MarkerOperator}.
     * @param id the operator ID
     * @param constant the enum constant attribute
     * @param <T> the attribute type
     * @return this
     */
    public <T extends Enum<T>> MockOperators marker(String id, T constant) {
        MarkerOperator operator = MarkerOperator.builder(TYPE)
                .attribute(String.class, id)
                .attribute(constant.getDeclaringClass(), constant)
                .build();
        return bless(id, operator);
    }

    /**
     * Registers an operator.
     * @param id the operator ID
     * @param operator the target operator
     * @return this
     */
    public MockOperators bless(String id, Operator operator) {
        operators.put(id, operator);
        return this;
    }

    /**
     * Connects between {@code <operator-id>.<port-name>} pairs.
     * @param upstream upstream port description
     * @param downstream downstream port description
     * @return this
     */
    public MockOperators connect(String upstream, String downstream) {
        OperatorOutput from = upstream(upstream);
        OperatorInput to = downstream(downstream);
        to.connect(from);
        return this;
    }

    /**
     * Validates if the operator exists.
     * @param id the operator id
     * @param kind the operator kind
     * @return this
     */
    public MockOperators assertOperator(String id, OperatorKind kind) {
        Operator operator = get(id);
        assertThat(operator.toString(), operator.getOperatorKind(), is(kind));
        return this;
    }

    /**
     * Validates if both are connected.
     * @param upstream upstream port description
     * @param downstream downstream port description
     * @return this
     */
    public MockOperators assertConnected(String upstream, String downstream) {
        return assertConnected(upstream, downstream, true);
    }

    /**
     * Validates if both are connected.
     * @param upstream upstream port description
     * @param downstream downstream port description
     * @param connected whether they are connected or not
     * @return this
     */
    public MockOperators assertConnected(String upstream, String downstream, boolean connected) {
        OperatorOutput from = upstream(upstream);
        OperatorInput to = downstream(downstream);
        assertThat(String.format("%s->%s", upstream, downstream), to.isConnected(from), is(connected));
        return this;
    }

    private OperatorOutput upstream(String expression) {
        String[] pair = pair(expression);
        Operator operator = get(pair[0]);
        OperatorOutput port;
        if (pair[1].equals("*")) {
            assertThat(operator.getOutputs(), hasSize(1));
            port = operator.getOutputs().get(0);
        } else {
            port = operator.findOutput(pair[1]);
        }
        assertThat(expression, port, is(notNullValue()));
        return port;
    }

    private OperatorInput downstream(String expression) {
        String[] pair = pair(expression);
        Operator operator = get(pair[0]);
        OperatorInput port;
        if (pair[1].equals("*")) {
            assertThat(operator.getInputs(), hasSize(1));
            port = operator.getInputs().get(0);
        } else {
            port = operator.findInput(pair[1]);
        }
        assertThat(expression, port, is(notNullValue()));
        return port;
    }

    private String[] pair(String pair) {
        int index = pair.indexOf('.');
        if (index < 0) {
            return new String[] { pair, "*" };
        }
        assertThat(index, is(greaterThan(0)));
        return new String[] { pair.substring(0, index), pair.substring(index + 1) };
    }

    /**
     * Returns operator.
     * @param id the operator id
     * @return the operator
     */
    public Operator get(String id) {
        assertThat(operators, hasKey(id));
        return operators.get(id);
    }

    /**
     * Returns input port.
     * @param expression port description
     * @return the port
     */
    public OperatorInput getInput(String expression) {
        return downstream(expression);
    }

    /**
     * Returns output port.
     * @param expression port description
     * @return the port
     */
    public OperatorOutput getOutput(String expression) {
        return upstream(expression);
    }

    /**
     * Returns all operators.
     * @return the operators
     */
    public Set<Operator> all() {
        return new HashSet<>(operators.values());
    }

    /**
     * Returns the id of the operator.
     * @param operator target operator (may not registered into this)
     * @return the ID
     */
    public String id(Operator operator) {
        String id = id0(operator);
        assertThat(operator.toString(), id, is(notNullValue()));
        return id;
    }

    /**
     * Returns set of operators.
     * @param ids operator IDs
     * @return operator set
     */
    public Set<Operator> getAsSet(String... ids) {
        Set<Operator> results = new LinkedHashSet<>();
        for (String id : ids) {
            results.add(get(id));
        }
        return results;
    }

    /**
     * Returns set of marker operators.
     * @param ids operator IDs
     * @return marker operator set
     */
    public Set<MarkerOperator> getMarkers(String... ids) {
        Set<MarkerOperator> results = new HashSet<>();
        for (String id : ids) {
            Operator operator = get(id);
            assertThat(operator, is(instanceOf(MarkerOperator.class)));
            results.add((MarkerOperator) operator);
        }
        return results;
    }

    private String id0(Operator operator) {
        switch (operator.getOperatorKind()) {
        case INPUT:
            return ((ExternalInput) operator).getName();
        case OUTPUT:
            return ((ExternalOutput) operator).getName();
        case FLOW:
            return ((FlowOperator) operator).getDescriptionClass().getName();
        case MARKER:
            return ((MarkerOperator) operator).getAttribute(String.class);
        case USER: {
            OperatorArgument arg = ((UserOperator) operator).findArgument(KEY_ARGUMENT);
            if (arg == null) {
                return null;
            }
            ValueDescription value = arg.getValue();
            if (value.getValueKind() != ValueKind.IMMEDIATE
                    || value.getValueType().equals(classOf(String.class)) == false) {
                return null;
            }
            return (String) ((ImmediateDescription) value).getValue();
        }
        default:
            throw new AssertionError(operator);
        }
    }

    /**
     * Creates {@link OperatorGraph}.
     * @return the operator graph
     */
    public OperatorGraph toGraph() {
        return new OperatorGraph(operators.values());
    }
}
