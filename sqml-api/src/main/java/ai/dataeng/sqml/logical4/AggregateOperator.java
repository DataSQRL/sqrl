package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.function.FunctionHandle;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import ai.dataeng.sqml.relation.ColumnReferenceExpression;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.basic.IntegerType;
import ai.dataeng.sqml.schema2.constraint.NotNull;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Aggregates incoming records by {@link AggregateOperator#groupByKeys} and computes the provided aggregate functions
 * {@link AggregateOperator#aggregates} for each group.
 * Emits a row on each group update. The row contains the groupByKeys and one column for each aggregate function to contain its aggregate value.
 *
 * This class uses {@link LinkedHashMap} for {@link AggregateOperator#groupByKeys} and {@link AggregateOperator#aggregates}
 * because the iteration order determines the schema.
 */
@Value
public class AggregateOperator extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    final LinkedHashMap<LogicalPlan.Column, ColumnReferenceExpression> groupByKeys;
    final LinkedHashMap<LogicalPlan.Column, Aggregation> aggregates;
    final LogicalPlan.Column[] schema;

    public AggregateOperator(LogicalPlan.RowNode input, LinkedHashMap<LogicalPlan.Column, ColumnReferenceExpression> groupByKeys,
                             LinkedHashMap<LogicalPlan.Column, Aggregation> aggregates) {
        super(input);
        this.groupByKeys = groupByKeys;
        this.aggregates = aggregates;
        schema = new LogicalPlan.Column[groupByKeys.size() + aggregates.size()];
        int offset = 0;
        for (LogicalPlan.Column col : Iterables.concat(groupByKeys.keySet(), aggregates.keySet())) {
            schema[offset++] = col;
        }
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[][]{schema};
    }

    @Override
    public StreamType getStreamType() {
        return StreamType.RETRACT;
    }

    public static AggregateOperator createAggregateAndPopulateTable(LogicalPlan.RowNode input, LogicalPlan.Table table,
                                                                    Map<Name, ColumnReferenceExpression> groupByKeys,
                                                                    Map<Name, Aggregation> aggregates) {
        LinkedHashMap<LogicalPlan.Column, ColumnReferenceExpression> keys = new LinkedHashMap<>(groupByKeys.size());
        for (Map.Entry<Name,ColumnReferenceExpression> entry : groupByKeys.entrySet()) {
            ColumnReferenceExpression cre = entry.getValue();
            keys.put(LogicalPlanUtil.copyColumnToTable(cre.getColumn(),table,entry.getKey(),true),cre);
        }
        LinkedHashMap<LogicalPlan.Column, Aggregation> aggregations = new LinkedHashMap<>(aggregates.size());
        for (Map.Entry<Name,Aggregation> entry : aggregates.entrySet()) {
            Name name = entry.getKey();
            Preconditions.checkArgument(table.getField(name)==null);
            LogicalPlan.Column col = new LogicalPlan.Column(name, table, 0,
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
