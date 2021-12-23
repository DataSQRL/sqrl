package org.apache.calcite.jdbc;

import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Collection;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.OrdersSchema;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;

public class DefaultRootSchema implements Schema {

  @Override
  public Table getTable(String s) {
    System.out.println("Getting table:" + s);
    return null;
  }

  @Override
  public Set<String> getTableNames() {
    return Set.of("orders");
  }

  @Override
  public RelProtoDataType getType(String s) {
    return null;
  }

  @Override
  public Set<String> getTypeNames() {
    return Set.of();
  }

  @Override
  public Collection<Function> getFunctions(String s) {
    return null;
  }

  @Override
  public Set<String> getFunctionNames() {
    return Set.of();
  }

  @Override
  public Schema getSubSchema(String s) {
    LogicalPlan logicalPlan = new LogicalPlan();
    SqrlToCalciteTableTranslator tableTranslator = new SqrlToCalciteTableTranslator();
    TableResolver tableResolver = new SchemaWalker(NamePath.parse("orders"), logicalPlan, tableTranslator);

    return new OrdersSchema(tableResolver);
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Set.of("orders");
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String s) {
    return null;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Schema snapshot(SchemaVersion schemaVersion) {
    return null;
  }
}
