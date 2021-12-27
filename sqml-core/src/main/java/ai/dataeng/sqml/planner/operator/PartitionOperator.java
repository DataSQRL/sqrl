package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import java.util.Arrays;

/**
 * Groups the input stream by the {@link PartitionOperator#groupByKeys}, orders each group by the {@link PartitionOperator#order},
 * and then emits up to {@link PartitionOperator#limit} elements with the rank value of each emitted element within their
 * group added as an additional column {@link PartitionOperator#rank}.
 *
 * TODO: support within partition joins
 */
public class PartitionOperator extends LogicalPlanImpl.RowNode<LogicalPlanImpl.RowNode> {

    final Column[] groupByKeys;
    final Order order;
    final int limit;
    final Column rank;

    final Column[] outputSchema;

    public PartitionOperator(LogicalPlanImpl.RowNode input, Column[] groupByKeys, Order order, int limit, Column rank) {
        super(input);
        this.groupByKeys = groupByKeys;
        this.order = order;
        this.limit = limit;
        this.rank = rank;

        Column[] inputSchema = getInput().getOutputSchema()[0];
        outputSchema = Arrays.copyOf(inputSchema,inputSchema.length+1);
        outputSchema[outputSchema.length-1]=rank;
    }

    @Override
    public Column[][] getOutputSchema() {
        return new Column[][]{outputSchema};
    }

    @Override
    public StreamType getStreamType() {
        return StreamType.RETRACT;
    }
}
