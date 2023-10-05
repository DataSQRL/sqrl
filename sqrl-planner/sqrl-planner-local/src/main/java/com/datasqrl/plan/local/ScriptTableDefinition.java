/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local;

import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.util.StreamUtil;
import java.util.Set;
import lombok.Getter;
import lombok.NonNull;

import java.util.Map;

@Getter
public class ScriptTableDefinition {

  private final Set<ScriptRelationalTable> shredTableMap;

  public ScriptTableDefinition(@NonNull Set<ScriptRelationalTable> shredTableMap) {
    this.shredTableMap = shredTableMap;
  }

  public PhysicalRelationalTable getBaseTable() {
    return StreamUtil.getOnlyElement(StreamUtil.filterByClass(shredTableMap, PhysicalRelationalTable.class)).get();
  }

}
