package org.apache.calcite.schema;

import ai.dataeng.sqml.planner.CalciteTableFactory;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The calcite schema that resolves arbitrary table paths and functions
 */
@AllArgsConstructor
public class SqrlSchema extends AbstractSqrlSchema {

  @Getter
  private final CalciteTableFactory tableFactory;

  @Override
  public Table getTable(String table) {
    return tableFactory.create(table).orElse(null);
  }
}
