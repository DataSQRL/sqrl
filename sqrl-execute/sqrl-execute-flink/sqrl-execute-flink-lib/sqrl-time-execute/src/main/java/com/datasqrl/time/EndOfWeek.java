package com.datasqrl.time;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Time window function that returns the end of week for the timestamp argument.
 * E.g. endOfWeek(parseTimestamp(2023-03-12T18:23:34.083Z)) returns the timestamp 2023-03-12T23:59:59.999999999Z
 */
public class EndOfWeek extends TimeTumbleWindowFunction {

  public EndOfWeek() {
    super(ChronoUnit.WEEKS, ChronoUnit.DAYS);
  }

  @Override
  public Instant eval(Instant instant, Long multiple, Long offset) {
    if (multiple == null) {
      multiple = 1L;
    }
    Preconditions.checkArgument(multiple == 1,
        "Time window width must be 1. Use endofDay instead for flexible window widths.");
    if (offset == null) {
      offset = 0L;
    }
    Preconditions.checkArgument(offset >= 0 && offset <= 6, "Invalid offset in days: %s", offset);

    ZonedDateTime time = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
    int daysToSubtract = time.getDayOfWeek().getValue() - 1 - offset.intValue();
    if (daysToSubtract < 0) {
      daysToSubtract = 7 + daysToSubtract;
    }
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS)
        .minusDays(daysToSubtract).plus(1, timeUnit).minusNanos(1)
        .toInstant();
  }


}
