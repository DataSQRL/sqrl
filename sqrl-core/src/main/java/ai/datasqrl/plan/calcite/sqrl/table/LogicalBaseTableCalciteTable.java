package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Field;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;

@Getter
public class LogicalBaseTableCalciteTable extends AbstractSqrlTable {

  private final SourceTableImport sourceTableImport;
  private final RelDataType type;
  private final NamePath shredPath;
  private final List<RelDataTypeField> fields;

  public LogicalBaseTableCalciteTable(SourceTableImport sourceTableImport, RelDataType type,
      NamePath shredPath) {
    this.sourceTableImport = sourceTableImport;
    this.type = type;
    this.fields = new ArrayList<>(type.getFieldList());
    this.shredPath = shredPath;
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
