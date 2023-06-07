package com.datasqrl.timestamp;


import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Clock;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class ProgressingMonotonicEventTimeWatermarks<T> implements WatermarkGenerator<T> {

  /** The maximum timestamp encountered so far. */
  private long maxTimestamp;

  /** The system time at which the last event was encountered */
  private long maxTimestampSystemTime;

  /** The maximum time in milliseconds by which an event can be delayed from a previously read event */
  private final long maxProgressDelayMillis;

  private final Clock clock;

  public ProgressingMonotonicEventTimeWatermarks(Duration progressDelay, Clock clock) {
    checkNotNull(progressDelay, "progressDelay");
    checkArgument(!progressDelay.isNegative(), "progressDelay cannot be negative");

    checkNotNull(clock,"clock");
    this.clock = clock;

    this.maxProgressDelayMillis = progressDelay.toMillis();

    // start so that our lowest watermark would be Long.MIN_VALUE.
    this.maxTimestamp = Long.MIN_VALUE + 1;
    this.maxTimestampSystemTime = 0;
  }

  @Override
  public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
    maxTimestamp = eventTimestamp;
    maxTimestampSystemTime = clock.millis();
  }

  @Override
  public void onPeriodicEmit(WatermarkOutput output) {
    long progressedTime = Long.MIN_VALUE;
    if (maxTimestampSystemTime > 0) {
      long elapsedTimeSinceLastEvent =
          clock.millis() - maxTimestampSystemTime - maxProgressDelayMillis;
      progressedTime = maxTimestamp + elapsedTimeSinceLastEvent -1;
    }
    output.emitWatermark(new Watermark(Math.max(maxTimestamp - 1, progressedTime)));
  }

  public static <T> WatermarkStrategy<T> of(Duration progressDelay) {
    return (ctx) -> {
      return new ProgressingMonotonicEventTimeWatermarks<>(progressDelay, Clock.systemUTC());
    };
  }


}
