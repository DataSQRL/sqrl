package ai.dataeng.sqml.logical4;

/**
 * Deduplicates an input stream.
 *
 * TODO: Not defined yet. Stub only.
 */
public class DistinctOperator extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    public DistinctOperator(LogicalPlan.RowNode input) {
        super(input);
    }



    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[0][];
    }

}
