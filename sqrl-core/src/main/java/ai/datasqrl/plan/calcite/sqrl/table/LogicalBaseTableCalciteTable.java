package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.name.NamePath;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

@Getter
@AllArgsConstructor
public class LogicalBaseTableCalciteTable extends AbstractSqrlTable {

  private final SourceTableImport sourceTableImport;
  private final RelDataType type;
  private final NamePath shredPath;

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return type;
  }
}
