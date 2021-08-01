package ai.dataeng.sqml.type;

import ai.dataeng.sqml.tree.Expression;
import java.util.Objects;

public abstract class SqmlType {
  protected final String name;

  private SqmlType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static abstract class ScalarSqmlType extends SqmlType {
    private ScalarSqmlType(String name) {
      super(name);
    }
  }

  public static class StringSqmlType extends ScalarSqmlType {
    public StringSqmlType() {
      super("STRING");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitString(this, context);
    }
  }

  public static class NumberSqmlType extends ScalarSqmlType {
    public NumberSqmlType() {
      super("NUMBER");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitNumber(this, context);
    }
  }

  public static class BooleanSqmlType extends ScalarSqmlType {
    public BooleanSqmlType() {
      super("BOOLEAN");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitBoolean(this, context);
    }
  }

  public static class ArraySqmlType extends SqmlType {

    private final SqmlType subType;

    public ArraySqmlType(SqmlType subType) {
      super("ARRAY");
      this.subType = subType;
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitArray(this, context);
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

  public static class UnknownSqmlType extends SqmlType {
    public UnknownSqmlType() {
      super("UNKNOWN");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitUnknown(this, context);
    }
  }

  public static class DateTimeSqmlType extends ScalarSqmlType {
    public DateTimeSqmlType() {
      super("DATE");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitDateTime(this, context);
    }
  }
  public static class NullSqmlType extends ScalarSqmlType {
    public NullSqmlType() {
      super("NULL");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitNull(this, context);
    }
  }


  public static class RelationSqmlType extends SqmlType {

    private final Expression expression;

    public RelationSqmlType(Expression expression) {
      super("RELATION");

      this.expression = expression;
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitRelation(this, context);
    }

    public Expression getExpression() {
      return expression;
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