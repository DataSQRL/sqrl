package com.datasqrl.plan.calcite;

/**
 * Bridges calcite with sqrl planning operations
 */
public interface SqrlCalciteBridge {

  org.apache.calcite.schema.Table getTable(String tableName);
}
