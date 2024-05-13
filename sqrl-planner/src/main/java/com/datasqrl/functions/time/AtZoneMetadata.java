package com.datasqrl.functions.time;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.function.InputPreservingFunction;
import com.datasqrl.time.AtZone;
import com.google.auto.service.AutoService;

@AutoService(FunctionMetadata.class)
public class AtZoneMetadata implements InputPreservingFunction {

  @Override
  public Class getMetadataClass() {
    return AtZone.class;
  }

  @Override
  public int preservedOperandIndex() {
    return 0;
  }
}
