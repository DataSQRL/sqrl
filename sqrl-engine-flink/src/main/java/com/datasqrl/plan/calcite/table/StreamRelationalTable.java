/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.table;

import com.datasqrl.plan.calcite.rel.LogicalStream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
public class StreamRelationalTable implements SourceRelationalTable {

  @Setter
  private String nameId;
  private final LogicalStream relNode;

  public StreamRelationalTable(LogicalStream relNode) {
    this(null,relNode);
  }

}
