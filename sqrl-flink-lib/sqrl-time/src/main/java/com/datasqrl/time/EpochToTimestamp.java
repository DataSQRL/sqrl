package com.datasqrl.time;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;

/**
 * Converts the epoch timestamp in seconds to the corresponding timestamp.
 * E.g. epochToTimestamp(1678645414) returns the timestamp 2023-03-12T18:23:34Z
 */
@AutoService(AutoRegisterSystemFunction.class)
public class EpochToTimestamp extends AbstractEpochToTimestamp {

  public EpochToTimestamp() {
    super(false);
  }

}