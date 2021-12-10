package ai.dataeng.sqml.optimizer;

import ai.dataeng.sqml.logical4.LogicalPlan;

public class MaterializeSink extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    final MaterializeSource source;

    public MaterializeSink(LogicalPlan.RowNode input, LogicalPlan.Table table) {
        super(input);
        assert getInput().getOutputSchema().length==1;
        LogicalPlan.Column[] tableSchema = getInput().getOutputSchema()[0].clone();
        this.source = new MaterializeSource(table, tableSchema);
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        assert getInput().getOutputSchema().length==1;
        return getInput().getOutputSchema();
    }

    public MaterializeSource getSource() {
        return source;
    }
}
