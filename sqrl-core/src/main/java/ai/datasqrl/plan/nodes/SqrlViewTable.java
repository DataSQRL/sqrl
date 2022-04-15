package ai.datasqrl.plan.nodes;

import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

public class SqrlViewTable extends AbstractTable {

  RelDataType relDataType;

  @Getter
  private final RelNode relNode;

  public SqrlViewTable(RelDataType relDataType, RelNode relNode) {
    this.relDataType = relDataType;
    this.relNode = relNode;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return relDataType;
  }

}
