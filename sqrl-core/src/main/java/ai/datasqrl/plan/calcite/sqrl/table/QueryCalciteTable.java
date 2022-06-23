package ai.datasqrl.plan.calcite.sqrl.table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

@Getter
@AllArgsConstructor
public class QueryCalciteTable extends AbstractSqrlTable {

  private final RelNode relNode;

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return relNode.getRowType();
  }
}
