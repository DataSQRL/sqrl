package com.datasqrl.functions.time;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.time.EndOfDay;
import com.datasqrl.time.EndOfMinute;
import com.datasqrl.time.TimeFunctions;
import com.google.auto.service.AutoService;

@AutoService(FunctionMetadata.class)
public class EndOfMinuteMetadata extends AbstractEndOfMetadata {

  public EndOfMinuteMetadata() {
    super(TimeFunctions.END_OF_MINUTE.getTimeUnit(),
        TimeFunctions.END_OF_MINUTE.getOffsetUnit());
  }

  @Override
  public Class getMetadataClass() {
    return EndOfMinute.class;
  }
}
