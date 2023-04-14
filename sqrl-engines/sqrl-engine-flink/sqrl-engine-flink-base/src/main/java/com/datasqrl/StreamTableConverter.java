/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.io.SourceRecord;
import com.datasqrl.model.LogicalStreamMetaData;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

@Value
public class StreamTableConverter {

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

  public static class EmitFirstInsertOrUpdate extends RichFlatMapFunction<Row, Row> {

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

    final LogicalStreamMetaData baseTbl;

    public Row apply(Row row, Long processingTimestamp) {
      int offset = 2;
      Object[] data = new Object[baseTbl.getSelectIdx().length + offset];
      data[0] = SourceRecord.makeUUID().toString();
//      data[1] = Instant.ofEpochMilli(processingTimestamp); //ingest time
      data[1] = row.getField(baseTbl.getTimestampIdx());
      int i = offset;
      for (int idx  : baseTbl.getSelectIdx()) {
        data[i++] = row.getField(idx);
      }
      return Row.ofKind(RowKind.INSERT, data);
    }
  }
}
