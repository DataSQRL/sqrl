package ai.dataeng.sqml.logical4;

import java.util.List;

public class DocumentComputeOperator extends LogicalPlan.DocumentNode {

    LogicalPlan.DocumentNode input;

    @Override
    List<LogicalPlan.DocumentNode> getInputs() {
        return List.of(input);
    }


}
