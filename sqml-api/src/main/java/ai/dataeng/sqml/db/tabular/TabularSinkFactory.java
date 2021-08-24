package ai.dataeng.sqml.db.tabular;

import ai.dataeng.sqml.db.DestinationTableSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

public interface TabularSinkFactory {

    SinkFunction<Row> getSink(String tableName, DestinationTableSchema schema);

}
