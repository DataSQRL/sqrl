package com.datasqrl.time;

/**
 * Converts the epoch timestamp in seconds to the corresponding timestamp. E.g.
 * epochToTimestamp(1678645414) returns the timestamp 2023-03-12T18:23:34Z
 */
public class EpochToTimestamp extends AbstractEpochToTimestamp {

  public EpochToTimestamp() {
    super(false);
  }
}
