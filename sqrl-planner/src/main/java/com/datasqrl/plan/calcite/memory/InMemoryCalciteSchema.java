package com.datasqrl.plan.calcite.memory;

import com.datasqrl.plan.calcite.memory.table.DataTable;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * An schema that holds data in-memory
 */
public class InMemoryCalciteSchema implements Schema {

  Map<String, DataTable> dataTableMap = new HashMap<>();

  @Override
  public Table getTable(String table) {
    return dataTableMap.get(table);
  }

  @Override
  public Set<String> getTableNames() {
    return dataTableMap.keySet();
  }

  public void registerDataTable(String name, DataTable table) {
    dataTableMap.put(name, table);
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String s) {
    return Schemas.subSchemaExpression(schemaPlus, s, this.getClass());
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
    return List.of();
  }

  @Override
  public Set<String> getFunctionNames() {
    return Set.of();
  }

  @Override
  public Schema getSubSchema(String s) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Set.of();
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Schema snapshot(SchemaVersion schemaVersion) {
    return this;
  }
}
