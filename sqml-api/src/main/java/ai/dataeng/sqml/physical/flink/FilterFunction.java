package ai.dataeng.sqml.physical.flink;

import ai.dataeng.sqml.relation.RowExpression;
import lombok.AllArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

@AllArgsConstructor
public class FilterFunction implements FlatMapFunction<RowUpdate,RowUpdate> {

    final RowExpression predicate;

    private boolean evaluate(Row row) {
        return true;
    }

    @Override
    public void flatMap(RowUpdate rowUpdate, Collector<RowUpdate> collector) throws Exception {
        if (rowUpdate instanceof RowUpdate.Simple) {
            if (evaluate(rowUpdate.getAddition())) collector.collect(rowUpdate);
        } else {
            RowUpdate.Full update = (RowUpdate.Full) rowUpdate;
            boolean addSatisfy = update.hasAddition() && evaluate(update.getAddition());
            boolean retractSatisfy = update.hasRetraction() && evaluate(update.getRetraction());
            if (addSatisfy || retractSatisfy) {
                collector.collect(new RowUpdate.Full(update.getIngestTime(),
                        addSatisfy?update.getAddition():null,
                        retractSatisfy?update.getRetraction():null));
            }
        }
    }
}
