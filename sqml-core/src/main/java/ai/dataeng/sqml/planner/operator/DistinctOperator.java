package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.planner.LogicalPlanImpl;

/**
 * Deduplicates an input stream.
 *
 * TODO: Not defined yet. Stub only.
 */
public class DistinctOperator extends LogicalPlanImpl.RowNode<LogicalPlanImpl.RowNode> {

    public DistinctOperator(LogicalPlanImpl.RowNode input) {
        super(input);
    }



    @Override
    public LogicalPlanImpl.Column[][] getOutputSchema() {
        return new LogicalPlanImpl.Column[0][];
    }

    @Override
    public StreamType getStreamType() {
        return StreamType.RETRACT;
    }
}
