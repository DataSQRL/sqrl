package ai.dataeng.sqml.function;

import ai.dataeng.sqml.type.SqmlType;

public class TypeSignature {

  private final String name;
  private final SqmlType type;

  public TypeSignature(String name, SqmlType type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public SqmlType getType() {
    return type;
  }
}
