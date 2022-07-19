package ai.datasqrl.plan.calcite;

import ai.datasqrl.parse.tree.name.Name;

/**
 * Bridges calcite with sqrl planning operations
 */
public interface SqrlCalciteBridge {

  org.apache.calcite.schema.Table getTable(String tableName);
}
