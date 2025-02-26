package com.datasqrl.time;

import java.time.temporal.ChronoUnit;

/**
 * Time window function that returns the end of second for the timestamp argument. E.g.
 * endOfSecond(parseTimestamp(2023-03-12T18:23:34.083Z)) returns the timestamp
 * 2023-03-12T18:23:34.999999999Z
 */
public class EndOfSecond extends TimeTumbleWindowFunction {

  public EndOfSecond() {
    super(ChronoUnit.SECONDS, ChronoUnit.MILLIS);
  }
}
