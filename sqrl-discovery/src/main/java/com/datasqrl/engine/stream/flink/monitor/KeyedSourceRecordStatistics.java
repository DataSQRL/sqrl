/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.monitor;

import com.datasqrl.engine.stream.flink.util.FlinkUtilities;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.stats.SourceTableStatistics;
import com.datasqrl.io.tables.TableSource;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Slf4j
public class KeyedSourceRecordStatistics extends
    KeyedProcessFunction<Integer, SourceRecord.Raw, SourceRecord.Raw> {

  public static final String STATE_NAME_SUFFIX = "-state";

  private final int maxRecords = 100000;
  private final int maxTimeInMin = 5;
  private final OutputTag<SourceTableStatistics> statsOutput;
  private final TableSource.Digest tableDigest;

  private transient ValueState<SourceTableStatistics> stats;
  private transient ValueState<Long> nextTimer;

  public KeyedSourceRecordStatistics(OutputTag<SourceTableStatistics> tag,
      TableSource.Digest tableDigest) {
    this.statsOutput = tag;
    this.tableDigest = tableDigest;
  }

  @Override
  public void processElement(SourceRecord.Raw sourceRecord, Context context,
      Collector<SourceRecord.Raw> out) throws Exception {
    SourceTableStatistics acc = stats.value();
    if (acc == null) {
      acc = new SourceTableStatistics();
      long timer =
          FlinkUtilities.getCurrentProcessingTime() + TimeUnit.MINUTES.toMillis(maxTimeInMin);
      nextTimer.update(timer);
      context.timerService().registerProcessingTimeTimer(timer);
      //Register an event timer into the far future to trigger when the stream ends
      context.timerService().registerEventTimeTimer(Long.MAX_VALUE);
    }
    ErrorCollector errors = new ErrorCollector(ErrorPrefix.INPUT_DATA);
    acc.validate(sourceRecord, tableDigest, errors);
    if (errors.isFatal()) {
      //TODO: Record is flawed, put it in sideoutput and issue warning; reuse MapWithErrorProcess
      log.error("Stats Validation Error: {}", errors);
    } else {
      acc.add(sourceRecord, tableDigest);
      stats.update(acc);
      if (acc.getCount() >= maxRecords) {
        context.timerService().deleteProcessingTimeTimer(nextTimer.value());
        context.output(statsOutput, acc);
        stats.clear();
        nextTimer.clear();
      }
      out.collect(sourceRecord);
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<SourceRecord.Raw> out)
      throws Exception {
    SourceTableStatistics acc = stats.value();
    if (acc != null) {
      ctx.output(statsOutput, acc);
    }
    stats.clear();
    nextTimer.clear();
  }

  @Override
  public void open(Configuration config) {
    ValueStateDescriptor<SourceTableStatistics> statsDesc =
        new ValueStateDescriptor<>(statsOutput.getId() + STATE_NAME_SUFFIX + ".data",
            TypeInformation.of(new TypeHint<SourceTableStatistics>() {
            }));
    stats = getRuntimeContext().getState(statsDesc);
    ValueStateDescriptor<Long> nextTimerDesc =
        new ValueStateDescriptor<>(statsOutput.getId() + STATE_NAME_SUFFIX + ".timer", Long.class);
    nextTimer = getRuntimeContext().getState(nextTimerDesc);
  }

}
