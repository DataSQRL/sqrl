package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.tree.name.NamePath;

import java.util.List;
import java.util.Map;

public class DocumentComputeOperator extends LogicalPlan.DocumentNode {

    LogicalPlan.DocumentNode input;

    @Override
    List<LogicalPlan.DocumentNode> getInputs() {
        return List.of(input);
    }

    @Override
    Map<NamePath, LogicalPlan.Column[]> getOutputSchema() {
        return null;
    }


}
