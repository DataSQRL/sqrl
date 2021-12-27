package ai.dataeng.sqml.catalog.persistence.tabular;

import ai.dataeng.sqml.catalog.persistence.DestinationTableSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

public interface TabularSinkFactory {

    SinkFunction<Row> getSink(String tableName, DestinationTableSchema schema);

}
