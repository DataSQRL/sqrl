package ai.dataeng.sqml.optimizer;

import ai.dataeng.sqml.logical4.LogicalPlan;
import lombok.Getter;
import lombok.Value;

import java.util.Collections;

@Value
public class MaterializeSource extends LogicalPlan.RowNode {

    final LogicalPlan.Table table;
    final LogicalPlan.Column[] tableSchema;

    public MaterializeSource(LogicalPlan.Table table, LogicalPlan.Column[] tableSchema) {
        super(Collections.EMPTY_LIST);
        this.tableSchema = tableSchema;
        this.table = table;
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[][]{tableSchema};
    }
}
