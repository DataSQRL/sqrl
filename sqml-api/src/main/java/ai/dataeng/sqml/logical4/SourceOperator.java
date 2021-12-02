package ai.dataeng.sqml.logical4;

import java.util.Collections;
import java.util.List;

public class SourceOperator extends LogicalPlan.DocumentNode {

    @Override
    List<LogicalPlan.DocumentNode> getInputs() {
        return Collections.EMPTY_LIST;
    }

}
