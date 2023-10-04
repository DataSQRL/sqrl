/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local;

import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.schema.SQRLTable;
import lombok.Getter;
import lombok.NonNull;

import java.util.Map;

@Getter
public class ScriptTableDefinition {

  private final PhysicalRelationalTable baseTable;
  private final Map<SQRLTable, VirtualRelationalTable> shredTableMap;

  public ScriptTableDefinition(@NonNull PhysicalRelationalTable baseTable,
      @NonNull Map<SQRLTable, VirtualRelationalTable> shredTableMap) {
    this.baseTable = baseTable;
    this.shredTableMap = shredTableMap;
  }

  public SQRLTable getTable() {
    return shredTableMap.entrySet().stream().filter(e -> e.getValue().isRoot())
        .map(Map.Entry::getKey).findFirst().get();
  }
}
