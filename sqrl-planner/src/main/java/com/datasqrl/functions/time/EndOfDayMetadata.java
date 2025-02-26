package com.datasqrl.functions.time;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.time.EndOfDay;
import com.datasqrl.time.TimeFunctions;
import com.google.auto.service.AutoService;

@AutoService(FunctionMetadata.class)
public class EndOfDayMetadata extends AbstractEndOfMetadata {

  public EndOfDayMetadata() {
    super(TimeFunctions.END_OF_DAY.getTimeUnit(), TimeFunctions.END_OF_DAY.getOffsetUnit());
  }

  @Override
  public Class getMetadataClass() {
    return EndOfDay.class;
  }
}
