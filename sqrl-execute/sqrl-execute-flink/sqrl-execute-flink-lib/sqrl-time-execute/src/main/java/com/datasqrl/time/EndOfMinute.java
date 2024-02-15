package com.datasqrl.time;

import java.time.temporal.ChronoUnit;

public class EndOfMinute extends TimeWindowBucketFunction {

  public EndOfMinute() {
    super(ChronoUnit.MINUTES, ChronoUnit.SECONDS);
  }

}
