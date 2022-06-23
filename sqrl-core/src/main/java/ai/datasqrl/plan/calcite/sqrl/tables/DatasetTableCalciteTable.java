package ai.datasqrl.plan.calcite.sqrl.tables;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

@Getter
@AllArgsConstructor
public class DatasetTableCalciteTable extends AbstractSqrlTable {

  private final SourceTableImport sourceTableImport;
  private final RelDataType type;

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return type;
  }
}