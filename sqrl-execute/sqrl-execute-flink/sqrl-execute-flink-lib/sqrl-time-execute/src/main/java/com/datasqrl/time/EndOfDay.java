package com.datasqrl.time;

import java.time.temporal.ChronoUnit;

public class EndOfDay extends TimeWindowBucketFunction {

  public EndOfDay() {
    super(ChronoUnit.DAYS, ChronoUnit.HOURS);
  }


}
