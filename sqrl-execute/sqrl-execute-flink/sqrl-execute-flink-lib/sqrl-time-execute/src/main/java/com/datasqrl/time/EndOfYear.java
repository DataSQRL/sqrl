package com.datasqrl.time;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

public class EndOfYear extends TimeWindowBucketFunction {

  public EndOfYear() {
    super(ChronoUnit.YEARS, ChronoUnit.DAYS);
  }

  public Instant eval(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
        .with(TemporalAdjusters.firstDayOfNextYear()).truncatedTo(ChronoUnit.DAYS).minusNanos(1).toInstant();
  }

  public Instant eval(Instant instant, Long multiple, Long offset) {
    if (multiple == null) {
      multiple = 1L;
    }
    Preconditions.checkArgument(multiple > 0 && multiple < Integer.MAX_VALUE,
        "Window width must be a positive integer value: %s", multiple);
    if (offset == null) {
      offset = 0L;
    }
    Preconditions.checkArgument(offset >= 0 && offset < 365, "Invalid offset in days: %s", offset);

    ZonedDateTime time = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
        .truncatedTo(ChronoUnit.DAYS);
    if (time.getDayOfYear() > offset) {
      time = time.with(TemporalAdjusters.firstDayOfNextYear());
    } else {
      time = time.with(TemporalAdjusters.firstDayOfYear());
    }
    int modulus = multiple.intValue();
    int yearsToAdd = (modulus - time.getYear() % modulus) % modulus;

    time = time.plusYears(yearsToAdd).plusDays(offset);
    return time.minusNanos(1).toInstant();
  }


}
