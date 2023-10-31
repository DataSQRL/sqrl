package com.datasqrl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Value;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.util.OutputTag;

@Value
public class PlanContext {

  public static final String ERROR_TAG_PREFIX = "_errors";


  List<SideOutputDataStream> errorStreams = new ArrayList<>();
  AtomicInteger errorTagCount = new AtomicInteger(0);

  public OutputTag<InputError> createErrorTag() {
    return new OutputTag<>(ERROR_TAG_PREFIX + "#" + errorTagCount.incrementAndGet()) {
    };
  }
}