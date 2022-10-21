package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

@Getter
public class Column extends Field {

  final boolean isVisible;
  private final RelDataType type;
  boolean nullable;

  public Column(Name name, int version, boolean isVisible, RelDataType type) {
    super(name, version);
    this.isVisible = isVisible;
    this.type = type;
    this.nullable = type.isNullable();
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  public FieldKind getKind() {
    return FieldKind.COLUMN;
  }

}
