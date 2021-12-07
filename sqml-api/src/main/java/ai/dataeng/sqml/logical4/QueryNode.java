package ai.dataeng.sqml.logical4;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class QueryNode extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    public QueryNode(LogicalPlan.RowNode input) {
        super(input);
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        assert getInput().getOutputSchema().length==1;
        return getInput().getOutputSchema();
    }

    public LogicalPlan.Table getTable() {
        return LogicalPlanUtil.getTable(getOutputSchema()[0]);
    }


}
