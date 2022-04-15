package ai.datasqrl.plan;

import ai.datasqrl.plan.nodes.SqrlViewTable;
import org.apache.calcite.rel.RelNode;

public class ViewFactory {

  public org.apache.calcite.schema.Table create(RelNode relNode) {
    return new SqrlViewTable(relNode.getRowType(), relNode);
  }
}
