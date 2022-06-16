package org.apache.calcite.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.SqrlCalciteBridge;
import org.apache.calcite.linq4j.tree.Expression;

/**
 * The calcite schema is intended to be a light proxy to the sqrl schema. Since the sqrl schema
 * is always in flux as the user defines the script, a mapping object is used to resolve the
 * current
 */
public class BridgedCalciteSchema extends AbstractSqrlSchema {
  SqrlCalciteBridge bridge;

  @Override
  public Table getTable(String table) {
    return bridge.getTable(Name.system(table));
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String s) {
    return Schemas.subSchemaExpression(schemaPlus, s, this.getClass());
//    return Expressions.call(DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method, new Expression[0]);
  }

  public void setBridge(SqrlCalciteBridge bridge) {
    this.bridge = bridge;
  }
}
