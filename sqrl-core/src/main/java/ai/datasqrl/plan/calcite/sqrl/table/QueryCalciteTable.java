package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.schema.Field;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;

@Getter
public class QueryCalciteTable extends AbstractSqrlTable {
  private final List<RelDataTypeField> fields;
  private final RelNode relNode;

  public QueryCalciteTable(RelNode relNode) {
    this.relNode = relNode;
    this.fields = new ArrayList<>(relNode.getRowType().getFieldList());
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return new RelRecordType(fields);
  }

  @Override
  public void addField(Field field, RelDataTypeField relDataTypeField) {
    fields.add(relDataTypeField);
  }
}
