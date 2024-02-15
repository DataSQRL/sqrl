package com.datasqrl.time;

import java.time.temporal.ChronoUnit;

public class EndOfHour extends TimeWindowBucketFunction {

  public EndOfHour() {
    super(ChronoUnit.HOURS, ChronoUnit.MINUTES);
  }

}
