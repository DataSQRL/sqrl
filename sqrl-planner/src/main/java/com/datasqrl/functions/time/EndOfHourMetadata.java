package com.datasqrl.functions.time;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.time.EndOfHour;
import com.datasqrl.time.TimeFunctions;
import com.google.auto.service.AutoService;

@AutoService(FunctionMetadata.class)
public class EndOfHourMetadata extends AbstractEndOfMetadata {

  public EndOfHourMetadata() {
    super(TimeFunctions.END_OF_HOUR.getTimeUnit(),
        TimeFunctions.END_OF_HOUR.getOffsetUnit());
  }

  @Override
  public Class getMetadataClass() {
    return EndOfHour.class;
  }
}
