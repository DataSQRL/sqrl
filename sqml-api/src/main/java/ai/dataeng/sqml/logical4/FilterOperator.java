package ai.dataeng.sqml.logical4;

import java.util.List;

public class FilterOperator extends LogicalPlan.RowNode {

    LogicalPlan.RowNode input;

    //Filter expression

    @Override
    List<LogicalPlan.RowNode> getInputs() {
        return List.of(input);
    }

    @Override
    LogicalPlan.Column[][] getOutputSchema() {
        //Filters do not change the records or their schema
        return input.getOutputSchema();
    }
}
