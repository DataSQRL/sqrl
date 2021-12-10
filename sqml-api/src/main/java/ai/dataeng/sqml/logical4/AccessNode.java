package ai.dataeng.sqml.logical4;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents query access or stream access to the input rows.
 *
 * Work in progress
 */
public class AccessNode extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    private final LogicalPlan.Table accessTable;
    private final List<LogicalPlan.Field> accessFields;
    private final Type accessType;

    public AccessNode(LogicalPlan.RowNode input, LogicalPlan.Table accessTable, List<LogicalPlan.Field> accessFields, Type accessType) {
        super(input);
        this.accessTable = accessTable;
        this.accessFields = accessFields;
        this.accessType = accessType;
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        assert getInput().getOutputSchema().length==1;
        return getInput().getOutputSchema();
    }

    public LogicalPlan.Table getTable() {
        return accessTable;
    }

    public static AccessNode forEntireTable(LogicalPlan.Table table, Type accessType) {
        final List<LogicalPlan.Field> visibleFields = new ArrayList<>();
        table.fields.visibleStream().forEach(f -> visibleFields.add(f));
        return new AccessNode(table.currentNode, table, visibleFields, accessType);
    }

    public enum Type {
        QUERY, STREAM;
    }
}
