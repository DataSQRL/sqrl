package org.apache.calcite.rel.type;

import org.apache.calcite.jdbc.SqrlToCalciteTableTranslator;
import java.util.Collection;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;

@AllArgsConstructor
public class CalciteSchema implements Schema {
    SqrlToCalciteTableTranslator tableTranslator;

  @Override
  public Table getTable(String table) {
    return tableTranslator.get(table);
  }

  @Override
  public Set<String> getTableNames() {
    return Set.of();
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
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Set.of();
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
