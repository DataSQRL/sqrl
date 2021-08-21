package ai.dataeng.sqml.type;

import ai.dataeng.sqml.analyzer.Field;
import java.io.Serializable;
import java.util.Objects;

public abstract class Type implements Serializable {
  protected final String name;

  protected Type(String name) {
    this.name = name;
  }

  public static Type fromName(String type) {
    switch (type) {
      case "integer": return IntegerType.INSTANCE;
      case "string": return StringType.INSTANCE;
      case "float": return FloatType.INSTANCE;
      case "datetime": return DateTimeType.INSTANCE;
    }
    return UnknownType.INSTANCE;
  }

  public String getName() {
    return name;
  }

  public Type combine(Type otherType) {
    if (this.equals(otherType)) return this;
    else return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Type type = (Type) o;
    return name.equals(type.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  public abstract <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context);

  public boolean isOrderable() {
    return true;
  }

  public boolean isComparable() {
    return true;
  }

  public Object getTypeSignature() {
    return "";
  }
}