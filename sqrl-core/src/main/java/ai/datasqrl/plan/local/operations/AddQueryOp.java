package ai.datasqrl.plan.local.operations;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Set;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class AddQueryOp implements SchemaUpdateOp {

  NamePath namePath;
  RelNode relNode;
  List<Name> fieldNames;
  Set<Integer> primaryKeys;
  Set<Integer> parentPrimaryKeys;

  @Override
  public <T> T accept(SchemaOpVisitor visitor) {
    return visitor.visit(this);
  }
}
