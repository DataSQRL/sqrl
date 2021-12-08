package ai.dataeng.sqml.logical4;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Represents query access or stream access to the input rows.
 *
 * Work in progress
 */
public class AccessNode extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    public AccessNode(LogicalPlan.RowNode input) {
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
