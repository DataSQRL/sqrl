package ai.dataeng.sqml.optimizer;

import ai.dataeng.sqml.logical4.LogicalPlan;

import java.util.Collections;
import java.util.List;

public class MaterializeSource extends LogicalPlan.RowNode {

    final LogicalPlan.Column[] tableSchema;
    final String tableName;

    public MaterializeSource(LogicalPlan.Column[] tableSchema, String tableName) {
        super(Collections.EMPTY_LIST);
        this.tableSchema = tableSchema;
        this.tableName = tableName;
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[][]{tableSchema};
    }
}
