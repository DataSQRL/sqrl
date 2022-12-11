package com.datasqrl.plan.calcite.table;

import com.datasqrl.engine.stream.flink.plan.StreamRelationalTableContext;
import com.datasqrl.engine.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.schema.UniversalTableBuilder;
import java.io.Serializable;
import java.time.Instant;
import lombok.Value;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

@Value
public class StreamTableSchemaStream {
  UniversalTableBuilder tblBuilder;

  public TypeInformation getTypeInformation() {
    TypeInformation typeInformation = new FlinkTypeInfoSchemaGenerator().convertSchema(
        tblBuilder);
    return typeInformation;
  }

  public DataStream convertToStream(StreamTableEnvironment tEnv,
      StreamRelationalTableContext ctx) {

    return tEnv.toChangelogStream(ctx.getInputTable())
        .filter(new ChangeFilter(ctx.getStateChangeType()))
        .process(new AugmentStream(), getTypeInformation());
  }

  @Value
  public static class ChangeFilter implements FilterFunction<Row>, Serializable {

    StateChangeType stateChangeType;

    @Override
    public boolean filter(Row row) throws Exception {
      RowKind kind = row.getKind();
      switch (stateChangeType) {
        case ADD:
          return kind == RowKind.INSERT;
        case UPDATE:
          return kind == RowKind.UPDATE_AFTER;
        case DELETE:
          return kind == RowKind.DELETE;
        default:
          throw new UnsupportedOperationException();
      }
    }
  }

  /**
   * Adds uuid, ingest time, and source time to the row
   */
  public static class AugmentStream extends ProcessFunction<Row, Row> {

    @Override
    public void processElement(Row row, ProcessFunction<Row, Row>.Context context,
        Collector<Row> collector) throws Exception {
      int offset = 2;
      Object[] data = new Object[row.getArity() + offset];
      data[0] = SourceRecord.makeUUID().toString();
      data[1] = Instant.ofEpochMilli(context.timerService().currentProcessingTime()); //ingest time
      //data[2] = Instant.ofEpochMilli(context.timestamp()); //source time //TODO: why is the timestamp not propagated?
      for (int i = 0; i < row.getArity(); i++) {
        data[i + offset] = row.getField(i);
      }
      collector.collect(Row.ofKind(RowKind.INSERT, data));
    }
  }

}
