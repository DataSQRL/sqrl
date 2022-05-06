package ai.datasqrl.plan.local.transpiler.nodes.relation;

import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Optional;

public abstract class RelationNorm extends Relation {
  protected RelationNorm(Optional<NodeLocation> location) {
    super(location);
  }

  public abstract RelationNorm getLeftmost();
  public abstract RelationNorm getRightmost();
  public abstract Name getFieldName(Expression references);
  public abstract List<Expression> getFields();
  public abstract List<Expression> getPrimaryKeys();
  public abstract Optional<Expression> walk(NamePath namePath);
}
