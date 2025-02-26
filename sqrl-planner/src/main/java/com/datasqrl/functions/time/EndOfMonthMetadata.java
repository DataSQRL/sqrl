package com.datasqrl.functions.time;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.time.EndOfMonth;
import com.datasqrl.time.TimeFunctions;
import com.google.auto.service.AutoService;

@AutoService(FunctionMetadata.class)
public class EndOfMonthMetadata extends AbstractEndOfMetadata {

  public EndOfMonthMetadata() {
    super(TimeFunctions.END_OF_MONTH.getTimeUnit(), TimeFunctions.END_OF_MONTH.getOffsetUnit());
  }

  @Override
  public Class getMetadataClass() {
    return EndOfMonth.class;
  }
}
