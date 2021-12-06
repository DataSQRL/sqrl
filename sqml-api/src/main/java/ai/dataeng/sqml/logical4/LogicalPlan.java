package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@link LogicalPlan} is a logical representation of the data flow that produces the tables and fields as defined
 * in an SQRL script.
 *
 * A {@link LogicalPlan} is collection of {@link Node}s that are connected into a DAG.
 */
public class LogicalPlan {

    public static final String TABLE_DELIMITER = "_";
    public static final String VERSION_DELIMITER = "v";

    /**
     * The {@link LogicalPlan#schema} represents the schema of the tables defined in an SQRL script.
     * The model is built incrementally and accounts for shadowing, i.e. adding elements to the schema with the same
     * name as previously added elements which makes those elements invisible to the API.
     *
     * The elements in the schema map to their corresponding elements in the {@link LogicalPlan}.
     * The schema produces the API schema. It is built incrementally while parsing the SQRL script and used
     * to resolve table and field references within the script.
     */
    ShadowingContainer<DatasetOrTable> schema = new ShadowingContainer<>();
    /**
     * All tables in the logical plan, not just those accessible through the schema
     */
    List<Table> allTables = new ArrayList<>();
    /**
     * All source nodes in the logical plan
     */
    List<Node> sourceNodes = new ArrayList<>();
    AtomicInteger tableIdCounter = new AtomicInteger(0);

    public static abstract class Node {

        List<Node> consumers;

        abstract List<? extends Node> getInputs();

        List<? extends Node> getConsumers() {
            return consumers;
        }

        public void addConsumer(Node node) {
            consumers.add(node);
        }

    }

    public static abstract class DocumentNode extends Node {

        abstract List<DocumentNode> getInputs();

        List<DocumentNode> getConsumers() {
            return (List)consumers;
        }

        abstract Map<NamePath,Column[]> getOutputSchema();

    }

    public static abstract class RowNode extends Node {

        List<RowNode> getConsumers() {
            return (List)consumers;
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
        abstract Column[][] getOutputSchema();

    }

    /**
     * Tables can be imported directly into the root scope of a script or an entire
     * dataset (with all tables) is imported and tables must be referenced through that dataset.
     */
    public interface DatasetOrTable extends ShadowingContainer.Nameable {

    }

    /**
     * It is not possible to define new tables inside a dataset (only in the root scope of the script)
     * so we don't have to consider shadowing of tables within a dataset.
     */
    public static class Dataset implements DatasetOrTable {

        final Name name;
        List<Table> tables = new ArrayList<>();

        public Dataset(Name name) {
            this.name = name;
        }

        @Override
        public Name getName() {
            return name;
        }
    }

    public static class Table implements DatasetOrTable {

        final Name name;
        final int uniqueId;
        ShadowingContainer<Field> fields = new ShadowingContainer<>();
        RowNode currentNode;

        private Table(int uniqueId, Name name) {
            this.name = name;
            this.uniqueId = uniqueId;
        }

        @Override
        public Name getName() {
            return name;
        }

        public ShadowingContainer<Field> getFields() {
            return fields;
        }

        public RowNode getCurrentNode() {
            return currentNode;
        }

        public void updateNode(RowNode node) {
            currentNode = node;
        }

        public String uniqueId2String() {
            return Integer.toHexString(uniqueId);
        }

        public String getId() {
            return name + TABLE_DELIMITER + uniqueId2String();
        }

    }

    public Table createTable(Name name) {
        Table table = new Table(tableIdCounter.incrementAndGet(), name);
        allTables.add(table);
        return table;
    }

    public static abstract class Field implements ShadowingContainer.Nameable {

        final Name name;

        protected Field(Name name) {
            this.name = name;
        }

        @Override
        public Name getName() {
            return name;
        }

    }

    public static class Column extends Field {
        //Identity of the column
        final Table table;
        final int version;

        //Column definition
        final BasicType type;
        final int arrayDepth;
        final List<Constraint> constraints;
        final boolean isPrimaryKey;

        public Column(Name name, Table table, int version,
                      BasicType type, int arrayDepth, List<Constraint> constraints,
                      boolean isPrimaryKey) {
            super(name);
            this.table = table;
            this.version = version;
            this.type = type;
            this.arrayDepth = arrayDepth;
            this.constraints = constraints;
            this.isPrimaryKey = isPrimaryKey;
        }

        public Column(Name name, Table table, int version,
                      BasicType type, int arrayDepth, List<Constraint> constraints) {
            this(name,table,version,type,arrayDepth,constraints,false);
        }

        public String getId() {
            String qualifiedName = name + TABLE_DELIMITER + table.uniqueId2String();
            if (version>0) qualifiedName += VERSION_DELIMITER + Integer.toHexString(version);
            return qualifiedName;
        }

    }



    @Getter
    public static class Relationship extends Field {

        final Table toTable;
        final Relationship.Type type;
        final Relationship.Multiplicity multiplicity;

        public Relationship(Name name, Table toTable, Type type, Multiplicity multiplicity) {
            super(name);
            this.toTable = toTable;
            this.type = type;
            this.multiplicity = multiplicity;
        }

        //captures the logical representation of the join that defines this relationship



        public enum Type {
            PARENT, CHILD, JOIN;
        }

        public enum Multiplicity {
            ZERO_ONE, ONE, MANY;
        }

    }

}
