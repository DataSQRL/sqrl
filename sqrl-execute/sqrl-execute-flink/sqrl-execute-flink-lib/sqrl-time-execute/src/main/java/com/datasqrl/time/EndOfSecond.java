package com.datasqrl.time;

import java.time.temporal.ChronoUnit;

public class EndOfSecond extends TimeWindowBucketFunction {

  public EndOfSecond() {
    super(ChronoUnit.SECONDS, ChronoUnit.MILLIS);
  }


}