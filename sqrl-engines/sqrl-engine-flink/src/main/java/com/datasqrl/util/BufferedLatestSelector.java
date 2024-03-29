/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class BufferedLatestSelector<T> extends KeyedProcessFunction<Integer, T, T> {

  public static final String STATE_NAME_SUFFIX = "-latest";

  private transient ValueState<T> latest;
  long bufferTimeMs;
  Class<T> clazz;
  String stateName;

  public BufferedLatestSelector(String stateName, long bufferTimeMS, Class<T> clazz) {
    this.bufferTimeMs = bufferTimeMs;
    this.clazz = clazz;
    this.stateName = stateName + STATE_NAME_SUFFIX;
  }

  @Override
  public void processElement(T value, Context context, Collector<T> out) throws Exception {
    if (latest.value() == null) {
      context.timerService()
          .registerProcessingTimeTimer(FlinkUtilities.getCurrentProcessingTime() + bufferTimeMs);
      //This event timer seems to cause an infinite loop but without it, there is no output
      context.timerService().registerEventTimeTimer(Long.MAX_VALUE);
    }
    latest.update(value);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<T> out) throws Exception {
    T value = latest.value();
    latest.clear();
    out.collect(value);
  }

  @Override
  public void open(Configuration config) {
    ValueStateDescriptor<T> descriptor = new ValueStateDescriptor<>(stateName, clazz);
    latest = getRuntimeContext().getState(descriptor);
  }

}