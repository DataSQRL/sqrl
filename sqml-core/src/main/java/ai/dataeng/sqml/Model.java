package ai.dataeng.sqml;

import ai.dataeng.sqml.plan.PlanNode;
import ai.dataeng.sqml.sql.tree.QualifiedName;
import java.util.Optional;

public class Model extends ModelRelation {

  public Model() {
    super("Root", Optional.empty());
  }


}
