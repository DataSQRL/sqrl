package org.apache.calcite.rel.type;

import ai.dataeng.sqml.planner.Column;
import java.io.Serializable;
import java.util.Objects;
import org.apache.calcite.sql.type.SqlTypeName;

public class SqrlCalciteField implements RelDataTypeField, Serializable {

  private final RelDataType type;
  private final String name;
  private final int index;
  private final Column column;

  public SqrlCalciteField(String name, int index, RelDataType type, Column column) {
    assert name != null;

    assert type != null;
    this.name = name;
    this.index = index;
    this.type = type;
    this.column = column;
  }

  public int hashCode() {
    return Objects.hash(new Object[]{this.index, this.name, this.type});
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof SqrlCalciteField)) {
      return false;
    } else {
      SqrlCalciteField that = (SqrlCalciteField) obj;
      return this.index == that.index && this.name.equals(that.name) && this.type.equals(that.type);
    }
  }

  public String getName() {
    return this.name;
  }

  public int getIndex() {
    return this.index;
  }

  public Column getColumn() {
    return column;
  }

  public RelDataType getType() {
    return this.type;
  }

  public final String getKey() {
    return this.getName();
  }

  public final RelDataType getValue() {
    return this.getType();
  }

  public RelDataType setValue(RelDataType value) {
    throw new UnsupportedOperationException();
  }

  public String toString() {
    return "#" + this.index + ": " + this.name + " " + this.type;
  }

  public boolean isDynamicStar() {
    return this.type.getSqlTypeName() == SqlTypeName.DYNAMIC_STAR;
  }
}