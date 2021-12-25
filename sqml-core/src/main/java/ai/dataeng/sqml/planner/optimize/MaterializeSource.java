package ai.dataeng.sqml.planner.optimize;

import ai.dataeng.sqml.planner.LogicalPlanImpl;
import lombok.Value;

import java.util.Collections;

@Value
public class MaterializeSource extends LogicalPlanImpl.RowNode {

    final LogicalPlanImpl.Table table;
    final LogicalPlanImpl.Column[] tableSchema;

    public MaterializeSource(LogicalPlanImpl.Table table, LogicalPlanImpl.Column[] tableSchema) {
        super(Collections.EMPTY_LIST);
        this.tableSchema = tableSchema;
        this.table = table;
    }

    @Override
    public LogicalPlanImpl.Column[][] getOutputSchema() {
        return new LogicalPlanImpl.Column[][]{tableSchema};
    }
}
