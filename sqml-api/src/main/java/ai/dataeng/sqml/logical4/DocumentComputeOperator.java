package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.tree.name.NamePath;

import java.util.List;
import java.util.Map;

public class DocumentComputeOperator extends LogicalPlan.DocumentNode<LogicalPlan.Node> {

    public DocumentComputeOperator(LogicalPlan.DocumentNode input) {
        super(input);
    }

    @Override
    public Map<NamePath, LogicalPlan.Column[]> getOutputSchema() {
        return null;
    }


}
