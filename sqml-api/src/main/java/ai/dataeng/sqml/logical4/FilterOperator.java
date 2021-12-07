package ai.dataeng.sqml.logical4;

import java.util.List;

public class FilterOperator extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    //Filter expression

    public FilterOperator(LogicalPlan.RowNode input) {
        super(input);
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        //Filters do not change the records or their schema
        return getInput().getOutputSchema();
    }
}
