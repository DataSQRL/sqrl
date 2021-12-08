package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class LogicalPlanUtil {

    public static <F extends LogicalPlan.Field> F getField(LogicalPlan.Table table, Name name) {
        LogicalPlan.Field f = table.fields.getByName(name);
        if (f == null) throw new NoSuchElementException("Could not find: " + name);
        return (F) f;
    }

    public static LogicalPlan.Relationship getChildRelationship(LogicalPlan.Table table, Name name) {
        LogicalPlan.Relationship r = getField(table, name);
        if (r.type != LogicalPlan.Relationship.Type.CHILD)
            throw new IllegalArgumentException("Not a child relationship: " + name);
        return r;
    }

    public static void appendOperatorForTable(LogicalPlan.Table table, LogicalPlan.RowNode addedNode) {
        table.currentNode.addConsumer(addedNode);
        table.updateNode(addedNode);
    }

    public static void appendNodeToTable(LogicalPlan.Table table, Function<LogicalPlan.RowNode, LogicalPlan.RowNode> nodeConstructor) {
        LogicalPlan.RowNode node = nodeConstructor.apply(table.currentNode);
        table.currentNode.addConsumer(node);
        table.updateNode(node);
    }

    public static boolean isSource(LogicalPlan.Node node) {
        return node instanceof DocumentSource;
    }

    public static LogicalPlan.Table getTable(LogicalPlan.Column[] inputs) {
        Preconditions.checkArgument(inputs!=null && inputs.length>0);
        final LogicalPlan.Table table = inputs[0].getTable();
        assert Arrays.stream(inputs).map(c -> c.getTable()).filter(t -> !t.equals(table)).count() == 0;
        return table;
    }


}
