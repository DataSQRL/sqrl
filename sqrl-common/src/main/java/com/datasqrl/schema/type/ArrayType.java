package com.datasqrl.schema.type;

import java.util.Objects;

public class ArrayType implements Type {

  private final Type subType;

  public ArrayType(Type subType) {
    this.subType = subType;
  }

  public Type getSubType() {
    return subType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ArrayType that = (ArrayType) o;
    return Objects.equals(subType, that.subType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subType) << 1 + 31;
  }

  @Override
  public String toString() {
    return "[" + subType.toString() + "]";
  }

  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitArrayType(this, context);
  }
}
