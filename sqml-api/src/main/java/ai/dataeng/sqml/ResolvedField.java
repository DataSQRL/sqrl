package ai.dataeng.sqml;

import ai.dataeng.sqml.type.SqmlType;
import java.util.Objects;

public class ResolvedField {

  private final String name;
  private final SqmlType type;
  private final boolean isNonNull;

  public ResolvedField(String name, SqmlType type, boolean isNonNull) {
    this.name = name;
    this.type = type;
    this.isNonNull = isNonNull;
  }

  public String getName() {
    return name;
  }

  public SqmlType getType() {
    return type;
  }

  public boolean isNonNull() {
    return isNonNull;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ResolvedField that = (ResolvedField) o;
    return isNonNull == that.isNonNull && name.equals(that.name) && type.equals(that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, isNonNull);
  }
}
