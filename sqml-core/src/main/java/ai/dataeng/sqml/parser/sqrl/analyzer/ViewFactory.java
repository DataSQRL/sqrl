package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.sqrl.schema.SqrlViewTable;
import org.apache.calcite.rel.RelNode;

public class ViewFactory {

  public org.apache.calcite.schema.Table create(RelNode relNode) {
    return new SqrlViewTable(relNode.getRowType(), relNode);
  }
}
