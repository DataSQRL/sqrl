package ai.datasqrl.plan.local.analyze.util;

import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.JoinDeclaration;
import ai.datasqrl.parse.tree.TableNode;

public class AstUtil {

  /**
   * left-deep sql tree
   */
  public static TableNode getTargetTable(JoinDeclaration joinDeclaration) {
    TableNode targetTableNode = joinDeclaration.getRelation() instanceof TableNode
        ? (TableNode) joinDeclaration.getRelation()
        : (TableNode) ((Join) joinDeclaration
            .getRelation()).getRight();

    return targetTableNode;
  }
}
