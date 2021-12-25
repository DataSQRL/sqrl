package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.planner.LogicalPlanImpl;

/**
 * Emits a new event row depending on the {@link SubscriptionOperator#type}:
 * <ul>
 *     <li>ON_INSERT: Emits a row if the incoming row is an insert (i.e. has no retraction)</li>
 *     <li>ON_DELETE: Emits a row if the incoming row is a delete (i.e. has no update)</li>
 *     <li>ON_UPDATE: Emits a row for every incoming row</li>
 * </ul>
 * This operator creates new rows as events for when the underlying table changes.
 */
public class SubscriptionOperator extends LogicalPlanImpl.RowNode<LogicalPlanImpl.RowNode> {

    final Type type;
    //information on which columns to put the new event uuid & timestamps

    public SubscriptionOperator(LogicalPlanImpl.RowNode input, Type type) {
        super(input);
        this.type = type;
    }

    @Override
    public LogicalPlanImpl.Column[][] getOutputSchema() {
        return new LogicalPlanImpl.Column[0][];
    }

    @Override
    public StreamType getStreamType() {
        return StreamType.APPEND;
    }

    public enum Type {
        ON_INSERT, ON_DELETE, ON_UPDATE;
    }
}
