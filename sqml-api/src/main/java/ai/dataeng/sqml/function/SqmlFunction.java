package ai.dataeng.sqml.function;

import ai.dataeng.sqml.type.Type;

public class SqmlFunction {
  private final String name;
  private final Type type;
  private final boolean isAggregation;

  public SqmlFunction(String name, Type type, boolean isAggregation) {
    this.name = name;
    this.type = type;
    this.isAggregation = isAggregation;
  }

  public TypeSignature getTypeSignature() {
    return new TypeSignature(name, type);
  }

  public Type getType() {
    return type;
  }

  public boolean isAggregation() {
    return isAggregation;
  }

  public String getName() {
    return name;
  }
}
