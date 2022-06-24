package org.apache.calcite.schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;


public abstract class AbstractSqrlSchema implements Schema {

  private Map<String, Function> functionMap = new HashMap<>();

  @Override
  public Set<String> getTableNames() {
    return null;
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
    Function function = functionMap.get(s);

    return List.of();
  }

  @Override
  public Set<String> getFunctionNames() {
    return functionMap.keySet();
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
    return this;
  }

  public void setFunctionMap(Map<String, Function> functionMap) {
    this.functionMap = functionMap;
  }
}
