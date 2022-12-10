/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.error.ErrorLocation;
import lombok.Value;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.OutputTag;

@Value
public class FlinkStreamHolder<T> implements StreamHolder<T> {

  private final FlinkStreamEngine.Builder builder;
  private final DataStream<T> stream;

  @Override
  public <R> StreamHolder<R> mapWithError(FunctionWithError<T, R> function, String errorName,
      ErrorLocation errorLocation, Class<R> clazz) {
    final OutputTag<ProcessError> errorTag = builder.getErrorTag(errorName);
    SingleOutputStreamOperator<R> result = stream.process(
        new MapWithErrorProcess<>(errorTag, function, errorLocation), TypeInformation.of(clazz));
    //TODO: improve error handling
    result.getSideOutput(errorTag).addSink(new PrintSinkFunction<>());
    return wrap(result);
  }

  @Override
  public void printSink() {
    stream.addSink(new PrintSinkFunction<>());
  }

  private <R> FlinkStreamHolder<R> wrap(DataStream<R> newStream) {
    return new FlinkStreamHolder<>(builder, newStream);
  }
}
