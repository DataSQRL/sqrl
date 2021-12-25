package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.tree.name.NamePath;
import lombok.Value;

import java.util.List;
import java.util.Map;

/**
 * {@link DocumentComputeOperator} is a type of projection operator that applies to document-structured records
 * only and it only adds columns to the record based on computing expressions confined to a single document.
 */
@Value
public class DocumentComputeOperator extends LogicalPlanImpl.DocumentNode<LogicalPlanImpl.Node> {

    final List<Computation> computations;

    public DocumentComputeOperator(LogicalPlanImpl.DocumentNode input, List<Computation> computations) {
        super(input);
        this.computations = computations;
    }

    @Override
    public Map<NamePath, LogicalPlanImpl.Column[]> getOutputSchema() {
        return null;
    }

    @Value
    public static class Computation {

        final NamePath path;
        LogicalPlanImpl.Column column;
        //TODO: How should we capture document expressions? We only need to compute those in Flink

    }

}
