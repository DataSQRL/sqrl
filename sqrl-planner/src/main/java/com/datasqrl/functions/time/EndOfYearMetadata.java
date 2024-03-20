package com.datasqrl.functions.time;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.time.EndOfYear;
import com.datasqrl.time.TimeFunctions;
import com.google.auto.service.AutoService;

@AutoService(FunctionMetadata.class)
public class EndOfYearMetadata extends AbstractEndOfMetadata {

  public EndOfYearMetadata() {
    super(TimeFunctions.END_OF_YEAR.getTimeUnit(),
        TimeFunctions.END_OF_YEAR.getOffsetUnit());
  }

  @Override
  public Class getMetadataClass() {
    return EndOfYear.class;
  }
}
