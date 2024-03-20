package com.datasqrl.time;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimeFunctions {
  public static final EpochToTimestamp EPOCH_TO_TIMESTAMP = new EpochToTimestamp();
  public static final EpochMilliToTimestamp EPOCH_MILLI_TO_TIMESTAMP = new EpochMilliToTimestamp();
  public static final TimestampToEpoch TIMESTAMP_TO_EPOCH = new TimestampToEpoch();
  public static final TimestampToEpochMilli TIMESTAMP_TO_EPOCH_MILLI = new TimestampToEpochMilli();
  public static final ParseTimestamp STRING_TO_TIMESTAMP = new ParseTimestamp();
  public static final TimestampToString TIMESTAMP_TO_STRING = new TimestampToString();
  public static final AtZone AT_ZONE = new AtZone();
  public static final EndOfSecond END_OF_SECOND = new EndOfSecond();
  public static final EndOfMinute END_OF_MINUTE = new EndOfMinute();
  public static final EndOfHour END_OF_HOUR = new EndOfHour();
  public static final EndOfDay END_OF_DAY = new EndOfDay();
  public static final EndOfWeek END_OF_WEEK = new EndOfWeek();
  public static final EndOfMonth END_OF_MONTH = new EndOfMonth();
  public static final EndOfYear END_OF_YEAR = new EndOfYear();
}
