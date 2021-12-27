package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import ai.dataeng.sqml.planner.operator.StreamType;
import ai.dataeng.sqml.planner.operator.relation.ColumnReferenceExpression;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@link LogicalPlanImpl} is a logical representation of the data flow that produces the tables and fields as defined
 * in an SQRL script.
 *
 * A {@link LogicalPlanImpl} is collection of {@link Node}s that are connected into a DAG.
 */
public class LogicalPlanImpl implements LogicalPlan {

    public static final String ID_DELIMITER = "_";

    /**
     * The {@link LogicalPlanImpl#schema} represents the schema of the tables defined in an SQRL script.
     * The model is built incrementally and accounts for shadowing, i.e. adding elements to the schema with the same
     * name as previously added elements which makes those elements invisible to the API.
     *
     * The elements in the schema map to their corresponding elements in the {@link LogicalPlanImpl}.
     * The schema produces the API schema. It is built incrementally while parsing the SQRL script and used
     * to resolve table and field references within the script.
     */
    public ShadowingContainer<DatasetOrTable> schema = new ShadowingContainer<>();
    /**
     * All source nodes in the logical plan
     */
    public List<Node> sourceNodes = new ArrayList<>();

    private final AtomicInteger tableIdCounter = new AtomicInteger(0);

    public List<Node> getSourceNodes() {
        return sourceNodes;
    }

    public DatasetOrTable getSchemaElement(Name name) {
        return schema.getByName(name);
    }

    //TODO move
    public AtomicInteger getTableIdCounter() {
        return tableIdCounter;
    }

    public static abstract class Node<I extends Node, C extends Node> {

        final protected List<C> consumers = new ArrayList<>();
        final protected List<I> inputs;

        protected Node(List<I> inputs) {
            this.inputs = inputs;
        }

        protected Node(I input) {
            this(Arrays.asList(input));
        }

        public abstract StreamType getStreamType();

        public List<I> getInputs() {
            return inputs;
        }

        public I getInput() {
            Preconditions.checkArgument(inputs.size()==1);
            return inputs.get(0);
        }

        public List<C> getConsumers() {
            return consumers;
        }

        public void addConsumer(C node) {
            consumers.add(node);
        }

        public boolean removeConsumer(C node) {
            return consumers.remove(node);
        }

        public void replaceConsumer(C old, C replacement) {
            if (!removeConsumer(old)) throw new NoSuchElementException(String.format("Not a valid consumer: %s", old));
            addConsumer(replacement);
        }

        public void replaceInput(I old, I replacement) {
            for (int i = 0; i < inputs.size(); i++) {
                if (inputs.get(i).equals(old)) {
                    inputs.set(i,replacement);
                    return;
                }
            }
            throw new NoSuchElementException("Could not find intput: " + old);
        }

    }

    public static abstract class DocumentNode<C extends Node> extends Node<DocumentNode,C> {

        public DocumentNode(List<DocumentNode> inputs) {
            super(inputs);
        }

        public DocumentNode(DocumentNode input) {
            super(input);
        }

        public abstract Map<NamePath,Column[]> getOutputSchema();

        public StreamType getStreamType() {
            return StreamType.APPEND;
        }

    }

    public static abstract class RowNode<I extends Node> extends Node<I,RowNode> {

        public RowNode(List<I> inputs) {
            super(inputs);
        }

        public RowNode(I input) {
            super(input);
        }

        /**
         * By default, we assume the {@link StreamType} is inherited from the inputs.
         * {@link RowNode}'s that change the {@link StreamType} need to overwrite this method.
         * @return
         */
        @Override
        public StreamType getStreamType() {
            for (I input : getInputs()) {
                if (input.getStreamType()==StreamType.RETRACT) return StreamType.RETRACT;
            }
            //Only if all inputs are EVENT is the resulting stream an EVENT stream
            return StreamType.APPEND;
        }

        /**
         * The schema of the records produced by this node.
         *
         * The first dimension of this double-array index the tables that are joined for the records.
         * If the records are not the result of a join, the returned array has length 1.
         *
         * The inner array contains the columns associated with the table index.
         *
         * The index into the flattened schema array matches the index into the produced record.
         * Hence, the length of the record equals the sum of the lengths of all the inner arrays.
         *
         * @return
         */
        public abstract Column[][] getOutputSchema();

        public ColumnReferenceExpression[] getOutputVariables() {
            return new ColumnReferenceExpression[0];
        }
    }

    public Table createTable(Name name, boolean isInternal) {
        Table table = new Table(tableIdCounter.incrementAndGet(), name, isInternal);
        schema.add(table);
        return table;
    }

    public ShadowingContainer<DatasetOrTable> getSchema() {
        return schema;
    }


}
