package com.datasqrl.time;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;

/**
 * Returns the seconds since epoch for the given timestamp.
 * E.g. timestampToEpoch(parseTimestamp(2023-03-12T18:23:34.083Z)) returns the number 1678645414
 */
@AutoService(AutoRegisterSystemFunction.class)
public class TimestampToEpoch extends AbstractTimestampToEpoch {

  public TimestampToEpoch() {
    super(false);
  }
}
