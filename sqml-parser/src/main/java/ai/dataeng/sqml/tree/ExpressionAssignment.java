package ai.dataeng.sqml.tree;

import java.util.List;
import java.util.Optional;

public class ExpressionAssignment extends Assignment {

  private final QualifiedName name;
  private final Expression expression;

  public ExpressionAssignment(Optional<NodeLocation> location,
      QualifiedName name, Expression expression) {
    super(location);
    this.name = name;
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

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExpressionAssignment(this, context);
  }

  @Override
  public QualifiedName getName() {
    return name;
  }

  public Expression getExpression() {
    return expression;
  }
}
