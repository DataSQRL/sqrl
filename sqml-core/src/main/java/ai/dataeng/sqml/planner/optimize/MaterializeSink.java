package ai.dataeng.sqml.planner.optimize;

import ai.dataeng.sqml.planner.LogicalPlanImpl;

public class MaterializeSink extends LogicalPlanImpl.RowNode<LogicalPlanImpl.RowNode> {

    final MaterializeSource source;

    public MaterializeSink(LogicalPlanImpl.RowNode input, LogicalPlanImpl.Table table) {
        super(input);
        assert getInput().getOutputSchema().length==1;
        LogicalPlanImpl.Column[] tableSchema = getInput().getOutputSchema()[0].clone();
        this.source = new MaterializeSource(table, tableSchema);
    }

    @Override
    public LogicalPlanImpl.Column[][] getOutputSchema() {
        assert getInput().getOutputSchema().length==1;
        return getInput().getOutputSchema();
    }

    public MaterializeSource getSource() {
        return source;
    }
}
