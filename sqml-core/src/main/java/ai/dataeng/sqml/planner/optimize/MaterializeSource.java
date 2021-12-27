package ai.dataeng.sqml.planner.optimize;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.Table;
import lombok.Value;

import java.util.Collections;

@Value
public class MaterializeSource extends LogicalPlanImpl.RowNode {

    final Table table;
    final Column[] tableSchema;

    public MaterializeSource(Table table, Column[] tableSchema) {
        super(Collections.EMPTY_LIST);
        this.tableSchema = tableSchema;
        this.table = table;
    }

    @Override
    public Column[][] getOutputSchema() {
        return new Column[][]{tableSchema};
    }
}
