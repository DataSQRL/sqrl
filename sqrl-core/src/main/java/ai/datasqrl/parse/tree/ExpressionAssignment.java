package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Optional;

public class ExpressionAssignment extends Assignment {

  private final Expression expression;
  private final String sql;

  public ExpressionAssignment(Optional<NodeLocation> location,
      NamePath name, Expression expression, String sql) {
    super(location, name);
    this.expression = expression;
    this.sql = sql;
  }

  public String getSql() {
    return sql;
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

  public Expression getExpression() {
    return expression;
  }
}
