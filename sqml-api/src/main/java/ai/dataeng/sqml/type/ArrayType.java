package ai.dataeng.sqml.type;

import java.util.Objects;

public class ArrayType extends Type {

  private final Type subType;

  public ArrayType(Type subType) {
    super("ARRAY");
    this.subType = subType;
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitArray(this, context);
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
    return Objects.equals(subType, that.subType) && Objects.equals(name, that.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(subType);
  }
}
