package ai.datasqrl.plan.local.transpiler.util;

import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import java.util.List;

public class JoinUtil {

  public static RelationNorm mergeJoins(RelationNorm fromNorm, List<JoinNorm> addlJoins) {
    RelationNorm relationNorm = fromNorm;

    for (JoinNorm joinNorm : addlJoins) {
      joinNorm.setLeft(relationNorm);
      relationNorm = joinNorm;
    }
    return relationNorm;
  }
}
