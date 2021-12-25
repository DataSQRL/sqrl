package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.planner.LogicalPlanImpl;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents query access or stream access to the input rows.
 *
 * Work in progress
 */
public class AccessNode extends LogicalPlanImpl.RowNode<LogicalPlanImpl.RowNode> {

    private final LogicalPlanImpl.Table accessTable;
    private final List<LogicalPlanImpl.Field> accessFields;
    private final Type accessType;

    public AccessNode(
        LogicalPlanImpl.RowNode input, LogicalPlanImpl.Table accessTable, List<LogicalPlanImpl.Field> accessFields, Type accessType) {
        super(input);
        this.accessTable = accessTable;
        this.accessFields = accessFields;
        this.accessType = accessType;
    }

    @Override
    public LogicalPlanImpl.Column[][] getOutputSchema() {
        assert getInput().getOutputSchema().length==1;
        return getInput().getOutputSchema();
    }

    public LogicalPlanImpl.Table getTable() {
        return accessTable;
    }

    public static AccessNode forEntireTable(LogicalPlanImpl.Table table, Type accessType) {
        final List<LogicalPlanImpl.Field> visibleFields = new ArrayList<>();
        table.fields.visibleStream().forEach(f -> visibleFields.add(f));
        return new AccessNode(table.currentNode, table, visibleFields, accessType);
    }

    public enum Type {
        QUERY, STREAM;
    }
}
