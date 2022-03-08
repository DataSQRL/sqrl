package ai.dataeng.sqml.planner.operator2;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import java.util.List;
import org.apache.calcite.plan.RelOptNode;

public interface SqrlRelNode extends RelOptNode {

  List<Field> getFields();
  public List<Column> getPrimaryKeys();
  public List<Column> getReferences();
}
