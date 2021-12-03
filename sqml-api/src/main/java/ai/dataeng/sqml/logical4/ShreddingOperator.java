package ai.dataeng.sqml.logical4;

import java.util.List;

public class ShreddingOperator extends LogicalPlan.Node {

    LogicalPlan.DocumentNode input;

    List<LogicalPlan.RowNode> getConsumers() {
        return (List)consumers;
    }

    @Override
    List<LogicalPlan.DocumentNode> getInputs() {
        return List.of(input);
    }


}
