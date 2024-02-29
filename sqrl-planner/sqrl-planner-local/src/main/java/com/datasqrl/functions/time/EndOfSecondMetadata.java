package com.datasqrl.functions.time;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.time.EndOfSecond;
import com.datasqrl.time.TimeFunctions;
import com.google.auto.service.AutoService;

@AutoService(FunctionMetadata.class)
public class EndOfSecondMetadata extends AbstractEndOfMetadata {

  public EndOfSecondMetadata() {
    super(TimeFunctions.END_OF_SECOND.getTimeUnit(),
        TimeFunctions.END_OF_SECOND.getOffsetUnit());
  }

  @Override
  public Class getMetadataClass() {
    return EndOfSecond.class;
  }
}
