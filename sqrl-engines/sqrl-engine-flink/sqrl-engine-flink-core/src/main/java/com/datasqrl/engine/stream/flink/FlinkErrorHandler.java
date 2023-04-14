package com.datasqrl.engine.stream.flink;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.OutputTag;

public class FlinkErrorHandler {

  public static final String ERROR_TAG_PREFIX = "_errors";
  private final List<DataStream<InputError>> errorStreams = new ArrayList<>();
  final AtomicInteger counter = new AtomicInteger(0);

  public OutputTag<InputError> getTag() {
    return new OutputTag<>(ERROR_TAG_PREFIX + "#" + counter.incrementAndGet()) {
    };
  }

  public void registerErrorStream(DataStream<InputError> errorStream) {
    errorStreams.add(errorStream);
  }

  public Optional<DataStream<InputError>> getErrorStream() {
    if (errorStreams.size()>0) {
      DataStream<InputError> combinedStream = errorStreams.get(0);
      if (errorStreams.size() > 1) {
        combinedStream = combinedStream.union(
            errorStreams.subList(1, errorStreams.size()).toArray(size -> new DataStream[size]));
      }
      return Optional.of(combinedStream);
    } else return Optional.empty();
  }

}
