package com.datasqrl.functions.time;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.time.EndOfWeek;
import com.datasqrl.time.TimeFunctions;
import com.google.auto.service.AutoService;

@AutoService(FunctionMetadata.class)
public class EndOfWeekMetadata extends AbstractEndOfMetadata {

  public EndOfWeekMetadata() {
    super(TimeFunctions.END_OF_WEEK.getTimeUnit(),
        TimeFunctions.END_OF_WEEK.getOffsetUnit());
  }

  @Override
  public Class getMetadataClass() {
    return EndOfWeek.class;
  }
}
