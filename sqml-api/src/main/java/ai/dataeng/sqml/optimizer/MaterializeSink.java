package ai.dataeng.sqml.optimizer;

import ai.dataeng.sqml.logical4.LogicalPlan;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MaterializeSink extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    final String tableName;

    public MaterializeSink(LogicalPlan.RowNode input, String tableName) {
        super(input);
        this.tableName = tableName;
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        assert getInput().getOutputSchema().length==1;
        return getInput().getOutputSchema();
    }

    public MaterializeSource getSource() {
        assert getInput().getOutputSchema().length==1;
        LogicalPlan.Column[] tableSchema = getInput().getOutputSchema()[0].clone();
        return new MaterializeSource(tableSchema,tableName);
    }
}
