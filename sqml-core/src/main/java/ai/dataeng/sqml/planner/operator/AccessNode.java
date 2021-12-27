package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.Table;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents query access or stream access to the input rows.
 *
 * Work in progress
 */
public class AccessNode extends LogicalPlanImpl.RowNode<LogicalPlanImpl.RowNode> {

    private final Table accessTable;
    private final List<Field> accessFields;
    private final Type accessType;

    public AccessNode(
        LogicalPlanImpl.RowNode input, Table accessTable, List<Field> accessFields, Type accessType) {
        super(input);
        this.accessTable = accessTable;
        this.accessFields = accessFields;
        this.accessType = accessType;
    }

    @Override
    public Column[][] getOutputSchema() {
        assert getInput().getOutputSchema().length==1;
        return getInput().getOutputSchema();
    }

    public Table getTable() {
        return accessTable;
    }

    public static AccessNode forEntireTable(Table table, Type accessType) {
        final List<Field> visibleFields = new ArrayList<>();
        table.fields.visibleStream().forEach(f -> visibleFields.add(f));
        return new AccessNode(table.currentNode, table, visibleFields, accessType);
    }

    public enum Type {
        QUERY, STREAM;
    }
}
