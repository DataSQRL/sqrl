package ai.datasqrl.schema.operations;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.Set;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class AddColumnOp implements SchemaOperation {

  private final NamePath pathToTable;
  private final Name name;
  private final RelNode relNode;
  private final Integer index;
  private final Set<Integer> primaryKeys;
  private final Set<Integer> parentPrimaryKeys;


  @Override
  public <T> T accept(OperationVisitor visitor) {
    return visitor.visit(this);
  }
}
