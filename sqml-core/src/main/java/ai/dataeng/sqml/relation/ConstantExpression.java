package ai.dataeng.sqml.relation;


import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.common.predicate.Primitives;
import ai.dataeng.sqml.common.type.Type;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

@Immutable
public final class ConstantExpression
    extends RowExpression
{
  private final Object value;
  private final Type type;

  public ConstantExpression(Object value, Type type)
  {
    requireNonNull(type, "type is null");
    if (value != null && !Primitives.wrap(type.getJavaType()).isInstance(value)) {
      throw new IllegalArgumentException(String.format("Object '%s' does not match type %s", value, type.getJavaType()));
    }
    this.value = value;
    this.type = type;
  }

  public Object getValue()
  {
    return value;
  }

  public boolean isNull()
  {
    return value == null;
  }

  @Override
  @JsonProperty
  public Type getType()
  {
    return type;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(value, type);
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ConstantExpression other = (ConstantExpression) obj;
    return Objects.equals(this.value, other.value) && Objects.equals(this.type, other.type);
  }

  @Override
  public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
  {
    return visitor.visitConstant(this, context);
  }
}