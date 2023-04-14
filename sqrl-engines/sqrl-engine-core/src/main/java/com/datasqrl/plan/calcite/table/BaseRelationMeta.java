package com.datasqrl.plan.calcite.table;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
public class BaseRelationMeta {

  int[] keyIdx;
  int[] selectIdx;
  int timestampIdx;

  public boolean hasTimestamp() {
    return timestampIdx >= 0;
  }

}
