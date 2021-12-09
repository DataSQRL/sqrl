package ai.dataeng.sqml.logical4;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents query access or stream access to the input rows.
 *
 * Work in progress
 */
public class AccessNode extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    private final List<LogicalPlan.Field> accessFields;
    private final Type accessType;

    public AccessNode(LogicalPlan.RowNode input, List<LogicalPlan.Field> accessFields, Type accessType) {
        super(input);
        this.accessFields = accessFields;
        this.accessType = accessType;
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        assert getInput().getOutputSchema().length==1;
        return getInput().getOutputSchema();
    }

    public LogicalPlan.Table getTable() {
        return LogicalPlanUtil.getTable(getOutputSchema()[0]);
    }

    public static AccessNode forEntireTable(LogicalPlan.Table table, Type accessType) {
        final List<LogicalPlan.Field> visibleFields = new ArrayList<>();
        table.fields.visibleStream().forEach(f -> visibleFields.add(f));
        return new AccessNode(table.currentNode, visibleFields, accessType);
    }

    public enum Type {
        QUERY, STREAM;
    }
}
