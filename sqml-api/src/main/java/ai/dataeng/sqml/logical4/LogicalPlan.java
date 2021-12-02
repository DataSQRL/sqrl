package ai.dataeng.sqml.logical4;

import lombok.AllArgsConstructor;

import java.util.List;

/**
 * The {@link LogicalPlan} is a logical representation of the data flow that produces the tables and fields as defined
 * in an SQRL script.
 *
 * A {@link LogicalPlan} is collection of {@link Node}s that are connected into a DAG.
 */
public class LogicalPlan {

    public static final String IDENTIFIER_DELIMITER = "_";



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

    public static abstract class ConversionNode extends Node {

        abstract List<DocumentNode> getInputs();

        List<RecordNode> getConsumers() {
            return (List)consumers;
        }

    }

    public static abstract class RecordNode extends Node {

        abstract List<RecordNode> getInputs();

        List<RecordNode> getConsumers() {
            return (List)consumers;
        }

    }

    @AllArgsConstructor
    public static class TableLabel extends RecordNode {

        RecordNode tableNode;
        Table table;
        TableLabel previousLabel;

        @Override
        List<RecordNode> getInputs() {
            return List.of(tableNode);
        }

        private int version() {
            if (previousLabel==null) return 0;
            else return previousLabel.version()+1;
        }

        public String getId() {
            return table.getId() + IDENTIFIER_DELIMITER + Integer.toHexString(version());
        }
    }


    public static class Table {

        int uniqueId;
        String name;
        TableLabel current;

        public TableLabel getCurrentTableNode() {
            return current;
        }

        public TableLabel getNextLabel(RecordNode input) {
            TableLabel label = new TableLabel(input, this, current);
            current = label;
            return label;
        }

        public String uniqueId2String() {
            return Integer.toHexString(uniqueId);
        }

        public String getId() {
            return name + IDENTIFIER_DELIMITER + uniqueId2String();
        }

    }


    public static class Column {

        Table table;
        String name;
        int version;

        public String getId() {
            return name + IDENTIFIER_DELIMITER + Integer.toHexString(version) + IDENTIFIER_DELIMITER + table.uniqueId2String();
        }

    }

    public static class Relationship {
        //captures the logical representation of the join that defines this relationship
    }

}
