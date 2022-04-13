package org.apache.calcite.schema;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SqrlSchema extends AbstractSqrlSchema {

  @Override
  public Table getTable(String s) {
    throw new RuntimeException("All tables are registered to calcite through the catalog " + s);
  }
}
