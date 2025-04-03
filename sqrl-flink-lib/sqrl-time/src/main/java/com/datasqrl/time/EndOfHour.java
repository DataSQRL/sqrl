package com.datasqrl.time;

import java.time.temporal.ChronoUnit;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;

/**
 * Time window function that returns the end of hour for the timestamp argument.
 * E.g. endOfHour(parseTimestamp(2023-03-12T18:23:34.083Z)) returns the timestamp 2023-03-12T18:59:59.999999999Z
 */
@AutoService(AutoRegisterSystemFunction.class)
public class EndOfHour extends TimeTumbleWindowFunction {

  public EndOfHour() {
    super(ChronoUnit.HOURS, ChronoUnit.MINUTES);
  }

}
