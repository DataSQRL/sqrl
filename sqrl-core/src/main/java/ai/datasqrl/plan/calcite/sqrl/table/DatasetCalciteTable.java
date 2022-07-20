package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.name.Name;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

@Getter
public class DatasetCalciteTable extends AbstractSqrlTable {

  private final SourceTableImport sourceTableImport;
  private final RelDataType rowType;

  public DatasetCalciteTable(@NonNull Name rootTableId,
                           SourceTableImport sourceTableImport, RelDataType rowType) {
    super(getTableId(rootTableId));
    this.sourceTableImport = sourceTableImport;
    this.rowType = rowType;
  }

  public static Name getTableId(Name rootTableId) {
    return rootTableId.suffix("I");
  }

  @Override
  public void addField(RelDataTypeField relDataTypeField) {

  }
}
