package org.apache.calcite.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.SqrlCalciteBridge;
import java.util.HashSet;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;

/**
 * The calcite schema is intended to be a light proxy to the sqrl schema. Since the sqrl schema
 * is always in flux as the user defines the script, a mapping object is used to resolve the
 * current
 */
@Slf4j
public class BridgedCalciteSchema extends AbstractSqrlSchema {
  SqrlCalciteBridge bridge;

  @Override
  public Table getTable(String table) {
    Table calciteTable = bridge.getTable(Name.system(table));
    if (calciteTable == null) {
      log.error("Could not find calcite table {}", table);
    }
    return calciteTable;
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String s) {
    return Schemas.subSchemaExpression(schemaPlus, s, this.getClass());
  }

  public void setBridge(SqrlCalciteBridge bridge) {
    this.bridge = bridge;
  }
}
