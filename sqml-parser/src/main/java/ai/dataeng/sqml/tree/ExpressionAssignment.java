package ai.dataeng.sqml.tree;

import java.util.List;
import java.util.Optional;

public class ExpressionAssignment extends Assignment {

  private final Expression expression;

  public ExpressionAssignment(Optional<NodeLocation> location, Expression expression) {
    super(location);
    this.expression = expression;
  }

  @Override
  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }

  @Override
  public String toString() {
    return null;
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExpressionAssignment(this, context);
  }
  public Expression getExpression() {
    return expression;
  }
}
