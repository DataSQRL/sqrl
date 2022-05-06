package ai.datasqrl.plan.local.transpiler.util;

import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionRewriter;
import ai.datasqrl.parse.tree.ExpressionTreeRewriter;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceOrdinal;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import java.util.Map;

public class ExpressionNormRewriter extends ExpressionRewriter {

  private final Map<RelationNorm, RelationNorm> normMapping;

  public ExpressionNormRewriter(Map<RelationNorm, RelationNorm> normMapping) {
    this.normMapping = normMapping;
  }

  @Override
  public Expression rewriteReferenceOrdinal(ReferenceOrdinal node, Object context,
      ExpressionTreeRewriter treeRewriter) {
    ReferenceOrdinal referenceOrdinal = new ReferenceOrdinal(node.getOrdinal());
    referenceOrdinal.setTable((QuerySpecNorm) normMapping.get(node.getTable()));
    return referenceOrdinal;
  }


  @Override
  public Expression rewriteResolvedColumn(ResolvedColumn node, Object context,
      ExpressionTreeRewriter treeRewriter) {
    return new ResolvedColumn(node.getIdentifier(), normMapping.get(node.getRelationNorm()), node.getColumn());
  }
}
