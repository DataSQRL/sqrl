package com.datasqrl.time;

import java.time.temporal.ChronoUnit;

/**
 * Time window function that returns the end of day for the timestamp argument.
 * E.g. endOfDay(parseTimestamp(2023-03-12T18:23:34.083Z)) returns the timestamp 2023-03-12T23:59:59.999999999Z
 */
public class EndOfDay extends TimeWindowBucketFunction {

  public EndOfDay() {
    super(ChronoUnit.DAYS, ChronoUnit.HOURS);
  }


}
