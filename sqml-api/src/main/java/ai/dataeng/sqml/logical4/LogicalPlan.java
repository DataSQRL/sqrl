package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.tree.Relation;
import lombok.Getter;

import java.util.List;
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

    private AtomicInteger tableIdCounter = new AtomicInteger(0);

    public static abstract class Node {

        List<Node> consumers;

        abstract List<? extends Node> getInputs();

        List<? extends Node> getConsumers() {
            return consumers;
        }

    }

    public static abstract class DocumentNode extends Node {

        abstract List<DocumentNode> getInputs();

        List<DocumentNode> getConsumers() {
            return (List)consumers;
        }

    }

    public static abstract class RowNode extends Node {

        abstract List<RowNode> getInputs();

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



    public static class Table {

        int uniqueId;
        String providedName;
        RowNode currentNode;

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
            return providedName + TABLE_DELIMITER + uniqueId2String();
        }

    }


    public static class Column {
        //Identity of the column
        Table table;
        String providedName;
        int version;

        //Column definition
        BasicType type;
        int arrayDepth;
        boolean notnull;

        public String getId() {
            String qualifiedName = providedName + TABLE_DELIMITER + table.uniqueId2String();
            if (version>0) qualifiedName += VERSION_DELIMITER + Integer.toHexString(version);
            return qualifiedName;
        }

    }

    @Getter
    public static class Relationship {

        Table toTable;
        Relationship.Type type;
        Relationship.Multiplicity multiplicity;

        //captures the logical representation of the join that defines this relationship



        public enum Type {
            PARENT, CHILD, JOIN;
        }

        public enum Multiplicity {
            ZERO_ONE, ONE, MANY;
        }

    }

}
