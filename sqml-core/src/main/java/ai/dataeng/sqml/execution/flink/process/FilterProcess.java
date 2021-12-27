package ai.dataeng.sqml.execution.flink.process;

import ai.dataeng.sqml.planner.operator.relation.RowExpression;
import lombok.AllArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

@AllArgsConstructor
public class FilterProcess implements FlatMapFunction<RowUpdate,RowUpdate> {

    final RowExpression predicate;

    private boolean evaluate(Row row) {
        return true;
    }

    @Override
    public void flatMap(RowUpdate rowUpdate, Collector<RowUpdate> collector) throws Exception {
        if (rowUpdate instanceof RowUpdate.AppendOnly) {
            if (evaluate(rowUpdate.getAppend())) collector.collect(rowUpdate);
        } else {
            RowUpdate.Full update = (RowUpdate.Full) rowUpdate;
            boolean appendSatisfy = update.hasAppend() && evaluate(update.getAppend());
            boolean retractSatisfy = update.hasRetraction() && evaluate(update.getRetraction());
            if (appendSatisfy || retractSatisfy) {
                collector.collect(new RowUpdate.Full(update.getIngestTime(),
                        appendSatisfy?update.getAppend():null,
                        retractSatisfy?update.getRetraction():null));
            }
        }
    }
}
