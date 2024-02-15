package com.datasqrl.time;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

public class EndOfMonth extends TimeWindowBucketFunction {

  public EndOfMonth() {
    super(ChronoUnit.MONTHS, ChronoUnit.DAYS);
  }

  public Instant eval(Instant instant, Long multiple, Long offset) {
    if (multiple == null) {
      multiple = 1L;
    }
    Preconditions.checkArgument(multiple == 1,
        "Time window width must be 1. Use endofDay instead for flexible window widths.");
    if (offset == null) {
      offset = 0L;
    }
    Preconditions.checkArgument(offset >= 0 && offset <= 28, "Invalid offset in days: %s", offset);

    ZonedDateTime time = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
        .truncatedTo(ChronoUnit.DAYS);
    if (time.getDayOfMonth() > offset) {
      time = time.with(TemporalAdjusters.firstDayOfNextMonth());
    } else {
      time = time.with(TemporalAdjusters.firstDayOfMonth());
    }
    time = time.plusDays(offset);
    return time.minusNanos(1).toInstant();
  }

}
