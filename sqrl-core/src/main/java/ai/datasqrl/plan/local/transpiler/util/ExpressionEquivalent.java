package ai.datasqrl.plan.local.transpiler.util;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceExpression;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceOrdinal;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedFunctionCall;

public class ExpressionEquivalent extends AstVisitor<Boolean, Expression> {
  public static boolean equals(Expression s, Expression t) {
    ExpressionEquivalent e = new ExpressionEquivalent();
    return s.accept(e, t);
  }

  @Override
  public Boolean visitResolvedColumn(ResolvedColumn node, Expression context) {
    return node == context;
  }

  @Override
  public Boolean visitResolvedFunctionCall(ResolvedFunctionCall node, Expression context) {
    return node == context;
  }

  @Override
  public Boolean visitReferenceOrdinal(ReferenceOrdinal node, Expression context) {
    if (node == context) return true;
    return node.getTable().getSelect().getSelectItems()
        .get(node.getOrdinal())
        .getExpression().accept(this, context);
  }

  @Override
  public Boolean visitReferenceExpression(ReferenceExpression node, Expression context) {
    if (node == context) return true;
    return node.getReferences().accept(this, context);
  }
}
