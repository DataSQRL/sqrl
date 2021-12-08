package ai.dataeng.sqml.logical4;

import java.util.Arrays;

/**
 * Groups the input stream by the {@link PartitionOperator#groupByKeys}, orders each group by the {@link PartitionOperator#order},
 * and then emits up to {@link PartitionOperator#limit} elements with the rank value of each emitted element within their
 * group added as an additional column {@link PartitionOperator#rank}.
 *
 * TODO: support within partition joins
 */
public class PartitionOperator extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    final LogicalPlan.Column[] groupByKeys;
    final Order order;
    final int limit;
    final LogicalPlan.Column rank;

    final LogicalPlan.Column[] outputSchema;

    public PartitionOperator(LogicalPlan.RowNode input, LogicalPlan.Column[] groupByKeys, Order order, int limit, LogicalPlan.Column rank) {
        super(input);
        this.groupByKeys = groupByKeys;
        this.order = order;
        this.limit = limit;
        this.rank = rank;

        LogicalPlan.Column[] inputSchema = getInput().getOutputSchema()[0];
        outputSchema = Arrays.copyOf(inputSchema,inputSchema.length+1);
        outputSchema[outputSchema.length-1]=rank;
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[][]{outputSchema};
    }

    @Override
    public StreamType getStreamType() {
        return StreamType.RETRACT;
    }
}
