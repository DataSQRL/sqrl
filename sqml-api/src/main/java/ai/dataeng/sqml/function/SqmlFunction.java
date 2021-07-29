package ai.dataeng.sqml.function;

import ai.dataeng.sqml.type.SqmlType;

public class SqmlFunction {
  private final String name;
  private final SqmlType type;
  private final boolean isAggregation;

  public SqmlFunction(String name, SqmlType type, boolean isAggregation) {
    this.name = name;
    this.type = type;
    this.isAggregation = isAggregation;
  }

  public TypeSignature getTypeSignature() {
    return new TypeSignature(name, null);
  }
}
