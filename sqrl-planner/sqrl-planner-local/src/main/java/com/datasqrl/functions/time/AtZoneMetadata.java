package com.datasqrl.functions.time;

import com.datasqrl.function.InputPreservingFunction;
import com.datasqrl.function.TimestampPreservingFunction;
import com.datasqrl.time.AtZone;

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
