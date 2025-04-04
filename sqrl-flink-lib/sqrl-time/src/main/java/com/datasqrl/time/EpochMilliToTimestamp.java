package com.datasqrl.time;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;

/**
 * Converts the epoch timestamp in milliseconds to the corresponding timestamp.
 * E.g. epochMilliToTimestamp(1678645414000) returns the timestamp 2023-03-12T18:23:34Z
 */
@AutoService(AutoRegisterSystemFunction.class)
public class EpochMilliToTimestamp extends AbstractEpochToTimestamp {

  public EpochMilliToTimestamp() {
    super(true);
  }
}
