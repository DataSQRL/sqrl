package ai.datasqrl.plan.local.operations;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.Set;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class AddColumnOp implements SchemaUpdateOp {

  private final NamePath pathToTable;
  private final Name name;
  private final RelNode relNode;
  private final Integer index;
  private final Set<Integer> primaryKeys;
  private final Set<Integer> parentPrimaryKeys;


  @Override
  public <T> T accept(SchemaOpVisitor visitor) {
    return visitor.visit(this);
  }
}
