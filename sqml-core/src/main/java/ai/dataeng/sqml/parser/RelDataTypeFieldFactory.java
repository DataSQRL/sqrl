package ai.dataeng.sqml.parser;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.jdbc.SqrlTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.SqrlRelDataTypeField;

@AllArgsConstructor
public class RelDataTypeFieldFactory {
  private final SqrlTypeFactory typeFactory;

  public RelDataTypeField create(FieldPath fieldPath, int index) {
    return create(fieldPath, fieldPath.getName(), index);
  }

  public RelDataTypeField create(FieldPath fieldPath, String name, int index) {
    return new SqrlRelDataTypeField(name, index, typeFactory.createSqrlType(fieldPath), fieldPath);
  }

  public List<RelDataTypeField> create(Table table, boolean addRels) {
    List<RelDataTypeField> fields = new ArrayList<>();
    for (Field field : table.getFields().visibleList()) {
      if (!addRels && field instanceof Relationship) continue;
      fields.add(create(FieldPath.of(field), field.getId().toString(), fields.size()));
    }
    return fields;
  }
}
