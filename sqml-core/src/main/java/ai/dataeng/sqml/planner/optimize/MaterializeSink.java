package ai.dataeng.sqml.planner.optimize;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.Table;

public class MaterializeSink extends LogicalPlanImpl.RowNode<LogicalPlanImpl.RowNode> {

    final MaterializeSource source;

    public MaterializeSink(LogicalPlanImpl.RowNode input, Table table) {
        super(input);
        assert getInput().getOutputSchema().length==1;
        Column[] tableSchema = getInput().getOutputSchema()[0].clone();
        this.source = new MaterializeSource(table, tableSchema);
    }

    @Override
    public Column[][] getOutputSchema() {
        assert getInput().getOutputSchema().length==1;
        return getInput().getOutputSchema();
    }

    public MaterializeSource getSource() {
        return source;
    }
}
