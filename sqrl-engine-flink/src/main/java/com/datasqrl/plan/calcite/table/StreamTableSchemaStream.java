/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.table;

import com.datasqrl.engine.stream.flink.plan.StreamRelationalTableContext;
import com.datasqrl.engine.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.schema.UniversalTable;
import java.io.Serializable;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

@Value
public class StreamTableSchemaStream {

  public static final Time DEFAULT_SESSION_WINDOW_TIME = Time.milliseconds(3);

  private static final ChangelogMode INSERT_UPDATE =
      ChangelogMode.newBuilder()
          .addContainedKind(RowKind.INSERT)
          .addContainedKind(RowKind.UPDATE_AFTER)
          .build();

  private static final ChangelogMode DELETE_ONLY =
      ChangelogMode.newBuilder()
          .addContainedKind(RowKind.DELETE)
          .build();

  UniversalTable tblBuilder;

  public TypeInformation getTypeInformation() {
    TypeInformation typeInformation = new FlinkTypeInfoSchemaGenerator().convertSchema(
        tblBuilder);
    return typeInformation;
  }

  public DataStream convertToStream(StreamTableEnvironment tEnv,
      StreamRelationalTableContext ctx) {
    Table inputTable = ctx.getInputTable();
    StateChangeType changeType = ctx.getStateChangeType();
    StreamRelationalTable.BaseTableMetaData baseTbl = ctx.getBaseTableMetaData();
    RowMapper rowMapper = new RowMapper(baseTbl);
    DataStream<Row> stream =  tEnv.toChangelogStream(inputTable, inputTable.getSchema().toSchema(), ChangelogMode.upsert());
//        .process(new Inspector("Raw data"));
    switch (ctx.getStateChangeType()) {
      case ADD:
        if (ctx.isUnmodifiedChangelog()) {
          //We can simply filter on RowKind
          stream = stream.filter(row -> row.getKind()==RowKind.INSERT);
        } else {
          //Only emit the first insert or update per key
          stream = stream.keyBy(new KeyedIndexSelector(baseTbl.getKeyIdx()))
              .flatMap(new EmitFirstInsertOrUpdate());
        }
        break;
      case DELETE:
        Preconditions.checkArgument(ctx.isUnmodifiedChangelog(),"Cannot create DELETE stream from modified state table. Invert filter and use ADD instead.");
        stream = stream.filter(row -> row.getKind()==RowKind.DELETE);
        break;
      case UPDATE:
        stream = stream.filter(row -> row.getKind()==RowKind.INSERT || row.getKind()==RowKind.UPDATE_AFTER);
        break;
      default:
        throw new UnsupportedOperationException("Unexpected state change type: " + ctx.getStateChangeType());
    }
    stream = stream.process(new ConvertToStream(rowMapper),getTypeInformation());
    return stream;
  }

  @AllArgsConstructor
  public static class KeyedIndexSelector implements KeySelector<Row,Row> {

    int[] keyIdx;

    @Override
    public Row getKey(Row row) throws Exception {
      Object[] data = new Object[keyIdx.length];
      for (int i = 0; i < keyIdx.length; i++) {
        data[i] = row.getField(keyIdx[i]);
      }
      return Row.of(data);
    }
  }


  @AllArgsConstructor
  public static class ConvertToStream extends ProcessFunction<Row, Row> {

    final RowMapper rowMapper;

    @Override
    public void processElement(Row row, ProcessFunction<Row, Row>.Context context,
        Collector<Row> collector) throws Exception {
      collector.collect(rowMapper.apply(row, context.timerService().currentProcessingTime()));
    }
  }

  @AllArgsConstructor
  public static class Inspector extends ProcessFunction<Row, Row> {

    private final String prefix;

    @Override
    public void processElement(Row row, ProcessFunction<Row, Row>.Context context,
        Collector<Row> collector) throws Exception {
      System.out.println(prefix + " [" + row + "]@" + context.timestamp());
      collector.collect(row);
    }
  }

  public class EmitFirstInsertOrUpdate extends RichFlatMapFunction<Row, Row> {

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ValueState<Boolean> hasFired;

    @Override
    public void flatMap(Row input, Collector<Row> out) throws Exception {
      if (input.getKind()==RowKind.INSERT || input.getKind()==RowKind.UPDATE_AFTER) {
        Boolean currentHasFired = hasFired.value();
        hasFired.update(true);
        if (currentHasFired == null || currentHasFired == false) {
          out.collect(input);
        }
      }
    }

    @Override
    public void open(Configuration config) {
      ValueStateDescriptor<Boolean> descriptor =
          new ValueStateDescriptor<>(
              "hasFired", // the state name
              TypeInformation.of(new TypeHint<Boolean>() {}), // type information
              Boolean.FALSE); // default value of the state, if nothing was set
      hasFired = getRuntimeContext().getState(descriptor);
    }
  }

  /**
   * Adds uuid, ingest time, and source time to the row and copies only the selected fields
   */
  @AllArgsConstructor
  public static class RowMapper implements Serializable {

    final StreamRelationalTable.BaseTableMetaData baseTbl;

    public Row apply(Row row, Long processingTimestamp) {
      int offset = baseTbl.hasTimestamp()?3:2;
      Object[] data = new Object[baseTbl.getSelectIdx().length + offset];
      data[0] = SourceRecord.makeUUID().toString();
      data[1] = Instant.ofEpochMilli(processingTimestamp); //ingest time
      if (baseTbl.hasTimestamp()) {
        data[2] = row.getField(baseTbl.getTimestampIdx());
      }
      int i = offset;
      for (int idx  : baseTbl.getSelectIdx()) {
        data[i++] = row.getField(idx);
      }
      return Row.ofKind(RowKind.INSERT, data);
    }
  }

}
