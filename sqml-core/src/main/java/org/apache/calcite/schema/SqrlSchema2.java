package org.apache.calcite.schema;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SqrlSchema2 extends AbstractSqrlSchema {

  @Override
  public Table getTable(String s) {
    throw new RuntimeException("All tables as registered to calcite through the catalog");
  }
}
