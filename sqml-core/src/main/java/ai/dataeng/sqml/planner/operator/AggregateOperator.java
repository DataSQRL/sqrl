package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.LogicalPlanUtil;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import ai.dataeng.sqml.planner.operator.relation.ColumnReferenceExpression;
import ai.dataeng.sqml.type.basic.BasicType;
import ai.dataeng.sqml.type.basic.IntegerType;
import ai.dataeng.sqml.type.constraint.NotNull;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import lombok.Value;

/**
 * Aggregates incoming records by {@link AggregateOperator#groupByKeys} and computes the provided aggregate functions
 * {@link AggregateOperator#aggregates} for each group.
 * Emits a row on each group update. The row contains the groupByKeys and one column for each aggregate function to contain its aggregate value.
 *
 * This class uses {@link LinkedHashMap} for {@link AggregateOperator#groupByKeys} and {@link AggregateOperator#aggregates}
 * because the iteration order determines the schema.
 */
@Value
public class AggregateOperator extends LogicalPlanImpl.RowNode<LogicalPlanImpl.RowNode> {

    final LinkedHashMap<LogicalPlanImpl.Column, ColumnReferenceExpression> groupByKeys;
    final LinkedHashMap<LogicalPlanImpl.Column, Aggregation> aggregates;
    final LogicalPlanImpl.Column[] schema;

    public AggregateOperator(LogicalPlanImpl.RowNode input, LinkedHashMap<LogicalPlanImpl.Column, ColumnReferenceExpression> groupByKeys,
                             LinkedHashMap<LogicalPlanImpl.Column, Aggregation> aggregates) {
        super(input);
        this.groupByKeys = groupByKeys;
        this.aggregates = aggregates;
        schema = new LogicalPlanImpl.Column[groupByKeys.size() + aggregates.size()];
        int offset = 0;
        for (LogicalPlanImpl.Column col : Iterables.concat(groupByKeys.keySet(), aggregates.keySet())) {
            schema[offset++] = col;
        }
    }

    @Override
    public LogicalPlanImpl.Column[][] getOutputSchema() {
        return new LogicalPlanImpl.Column[][]{schema};
    }

    @Override
    public StreamType getStreamType() {
        return StreamType.RETRACT;
    }

    public static AggregateOperator createAggregateAndPopulateTable(LogicalPlanImpl.RowNode input, LogicalPlanImpl.Table table,
                                                                    Map<Name, ColumnReferenceExpression> groupByKeys,
                                                                    Map<Name, Aggregation> aggregates) {
        LinkedHashMap<LogicalPlanImpl.Column, ColumnReferenceExpression> keys = new LinkedHashMap<>(groupByKeys.size());
        for (Map.Entry<Name,ColumnReferenceExpression> entry : groupByKeys.entrySet()) {
            ColumnReferenceExpression cre = entry.getValue();
            keys.put(LogicalPlanUtil.copyColumnToTable(cre.getColumn(),table,entry.getKey(),true),cre);
        }
        LinkedHashMap<LogicalPlanImpl.Column, Aggregation> aggregations = new LinkedHashMap<>(aggregates.size());
        for (Map.Entry<Name,Aggregation> entry : aggregates.entrySet()) {
            Name name = entry.getKey();
            Preconditions.checkArgument(table.getField(name)==null);
            LogicalPlanImpl.Column col = new LogicalPlanImpl.Column(name, table, 0,
                    entry.getValue().functionHandle.getDataType(),
                    0,List.of(NotNull.INSTANCE),false, false);
            aggregations.put(col,entry.getValue());
        }
        AggregateOperator agg = new AggregateOperator(input, keys, aggregations);
        table.updateNode(agg);
        return agg;
    }

    @Value
    public static class Aggregation {

        //FunctionHandle functionHandle;
        AggregateFunction functionHandle;
        List<ColumnReferenceExpression> arguments;

    }

    /**
     * This is temporary until {@link FunctionHandle} is implemented
     */
    public enum AggregateFunction {

        COUNT(IntegerType.INSTANCE), SUM(IntegerType.INSTANCE);

        final BasicType dataType;

        AggregateFunction(BasicType dataType) {
            this.dataType = dataType;
        }

        public BasicType getDataType() {
            return dataType;
        }

        public int getNumArguments() {
            return 1;
        }
    }
}
