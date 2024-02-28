package com.datasqrl.time;

/**
 * Returns the seconds since epoch for the given timestamp.
 * E.g. timestampToEpoch(parseTimestamp(2023-03-12T18:23:34.083Z)) returns the number 1678645414
 */
public class TimestampToEpochMilli extends AbstractTimestampToEpoch {

  public TimestampToEpochMilli() {
    super(true);
  }
}

