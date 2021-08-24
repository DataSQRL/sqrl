package ai.dataeng.sqml.db.tabular;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

public class RowMapFunction implements FlatMapFunction<Tuple2<Boolean, Row>,Row> {

    @Override
    public void flatMap(Tuple2<Boolean, Row> input, Collector<Row> collector) throws Exception {
        if (input.f0.equals(Boolean.TRUE)) {
            Row row = input.f1;
            RowKind kind = row.getKind();
            if (kind == RowKind.UPDATE_AFTER || kind == RowKind.INSERT) {
                collector.collect(row);
            }
        }
    }
}
