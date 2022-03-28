package ai.dataeng.sqml.type;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.RelDataTypeConverter;
import ai.dataeng.sqml.tree.Assignment;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.basic.BasicType;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

@Getter
public class CalciteDelegatingField extends Field {

  private final boolean isHidden;
  private final boolean isInternal;
  private final boolean primaryKey;
  private final boolean parentPrimaryKey;
  private final RelDataTypeField field;

  public CalciteDelegatingField(Name name, boolean isHidden, boolean isInternal, boolean primaryKey,
      boolean parentPrimaryKey, RelDataTypeField field) {
    super(name, null);

    this.isHidden = isHidden;
    this.isInternal = isInternal;
    this.primaryKey = primaryKey;
    this.parentPrimaryKey = parentPrimaryKey;
    this.field = field;
  }

  public static ai.dataeng.sqml.parser.Field of(int index, RelDataType type) {
    return of(index, false, false, false, false, type);
  }

  public static ai.dataeng.sqml.parser.Field of(int index, boolean isInternal, boolean isHidden, boolean primaryKey, boolean parentPrimaryKey,
      RelDataType type) {
    RelDataTypeField field = type.getFieldList().get(index);

    return of(Name.system(field.getName()), isInternal, isHidden, primaryKey, parentPrimaryKey, field);
  }
  public static ai.dataeng.sqml.parser.Field of(Name name, boolean isInternal, boolean isHidden,
      boolean primaryKey, boolean parentPrimaryKey,
      RelDataTypeField field) {
    return new CalciteDelegatingField(name, isInternal, isHidden, primaryKey, parentPrimaryKey, field);
  }

  @Override
  public int getVersion() {
    return 0;
  }

  public BasicType getType() {
    return RelDataTypeConverter.toBasicType(field.getType());
  }
}
