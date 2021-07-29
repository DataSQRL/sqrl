package ai.dataeng.sqml.type;

import java.util.Objects;

public abstract class SqmlType {
  protected final String name;

  private SqmlType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static class StringSqmlType extends SqmlType {
    public StringSqmlType() {
      super("STRING");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  public static abstract class NumberSqmlType extends SqmlType {
    private NumberSqmlType(String name) {
      super(name);
    }
  }


  public static class FloatSqmlType extends NumberSqmlType {
    public FloatSqmlType() {
      super("FLOAT");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }
  public static class BooleanSqmlType extends NumberSqmlType {
    public BooleanSqmlType() {
      super("BOOLEAN");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  public static class IntegerSqmlType extends NumberSqmlType {
    public IntegerSqmlType() {
      super("Integer");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }


  public static class ArraySqmlType extends SqmlType {

    private final SqmlType subType;

    public ArraySqmlType(SqmlType subType) {
      super("ARRAY");
      this.subType = subType;
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }

    public SqmlType getSubType() {
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
      ArraySqmlType that = (ArraySqmlType) o;
      return Objects.equals(subType, that.subType) && Objects.equals(name, that.getName());
    }

    @Override
    public int hashCode() {
      return Objects.hash(subType);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SqmlType type = (SqmlType) o;
    return name.equals(type.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  public abstract <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context);
}