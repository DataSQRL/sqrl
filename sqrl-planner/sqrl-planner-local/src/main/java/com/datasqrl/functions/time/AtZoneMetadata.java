package com.datasqrl.functions.time;

import com.datasqrl.function.TimestampPreservingFunction;
import com.datasqrl.time.AtZone;

public class AtZoneMetadata implements TimestampPreservingFunction {

  @Override
  public Class getMetadataClass() {
    return AtZone.class;
  }
}
