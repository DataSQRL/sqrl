package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;

@Getter
public class SourceTableCalciteTable extends AbstractSqrlTable {

  private final SourceTableImport sourceTableImport;
  private final RelDataType type;
  private final List<RelDataTypeField> fields;

  public SourceTableCalciteTable(SourceTableImport sourceTableImport, RelDataType type) {
    this.sourceTableImport = sourceTableImport;
    this.type = type;
    this.fields = new ArrayList<>(type.getFieldList());
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return new RelRecordType(fields);
  }

  @Override
  public void addField(RelDataTypeField relDataTypeField) {
    fields.add(relDataTypeField);
  }
}