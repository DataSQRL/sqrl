package ai.dataeng.sqml.physical.flink;

import ai.dataeng.sqml.relation.RowExpression;
import lombok.AllArgsConstructor;
import org.apache.flink.types.Row;

@AllArgsConstructor
public class FilterFunction implements org.apache.flink.api.common.functions.FilterFunction<Row> {

    final RowExpression predicate;

    @Override
    public boolean filter(Row row) throws Exception {
        return true; //TODO: evaluate predicate against row
    }
}
